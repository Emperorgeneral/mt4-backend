"""
MT4 EA Message Broker Backend
A lightweight, reliable message relay system designed specifically for MT4 EAs.

Features:
- Simple POST/GET endpoints
- In-memory message queue with TTL
- One-time message delivery
- Auto-cleanup of expired messages
- No session management, no heartbeat dependencies
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from datetime import datetime, timedelta
from collections import defaultdict
import threading
import uuid
import logging
import os


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="MT4 Message Broker", version="1.0.0")

# Enable CORS for MT4 WebRequest
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# Configuration
# ============================================================================

MESSAGE_TTL_SECONDS = 300  # Messages expire after 5 minutes
CLEANUP_INTERVAL_SECONDS = 60  # Cleanup runs every minute

# ============================================================================
# Data Models
# ============================================================================

class Message(BaseModel):
    """Message structure for EA communication"""
    from_ea: str = Field(..., description="Source EA identifier")
    to_ea: str = Field(..., description="Target EA identifier")
    type: str = Field(..., description="Message type/action")
    payload: Dict = Field(default_factory=dict, description="Message data")
    timestamp: Optional[str] = Field(None, description="ISO format timestamp")
    message_id: Optional[str] = Field(None, description="Unique message ID")
    expires_at: Optional[datetime] = Field(None, description="Expiration time")

class MessageResponse(BaseModel):
    """Response for successful operations"""
    status: str
    message_id: str
    timestamp: str

class MessagesResponse(BaseModel):
    """Response for polling messages"""
    status: str
    count: int
    messages: List[Message]

# ============================================================================
# Message Storage (In-Memory)
# ============================================================================

class MessageQueue:
    """Thread-safe in-memory message queue with TTL"""
    
    def __init__(self):
        self.messages: Dict[str, List[Message]] = defaultdict(list)
        self.lock = threading.Lock()
        logger.info("MessageQueue initialized")
    
    def add_message(self, msg: Message) -> str:
        """Add a message to the queue"""
        with self.lock:
            # Generate unique ID and timestamp
            msg.message_id = str(uuid.uuid4())
            msg.timestamp = datetime.utcnow().isoformat()
            msg.expires_at = datetime.utcnow() + timedelta(seconds=MESSAGE_TTL_SECONDS)
            
            # Store message for target EA
            self.messages[msg.to_ea].append(msg)
            
            logger.info(f"Message added: {msg.from_ea} → {msg.to_ea} | Type: {msg.type} | ID: {msg.message_id}")
            return msg.message_id
    
    def get_messages(self, ea_name: str) -> List[Message]:
        """Get and remove all messages for an EA (one-time delivery)"""
        with self.lock:
            if ea_name not in self.messages or not self.messages[ea_name]:
                return []
            
            # Get all messages for this EA
            messages = self.messages[ea_name]
            
            # Clear the queue (one-time delivery)
            self.messages[ea_name] = []
            
            logger.info(f"Messages delivered to {ea_name}: {len(messages)} message(s)")
            return messages
    
    def cleanup_expired(self):
        """Remove expired messages"""
        with self.lock:
            now = datetime.utcnow()
            removed_count = 0
            
            for ea_name in list(self.messages.keys()):
                original_count = len(self.messages[ea_name])
                
                # Keep only non-expired messages
                self.messages[ea_name] = [
                    msg for msg in self.messages[ea_name]
                    if msg.expires_at and msg.expires_at > now
                ]
                
                removed = original_count - len(self.messages[ea_name])
                removed_count += removed
                
                # Clean up empty queues
                if not self.messages[ea_name]:
                    del self.messages[ea_name]
            
            if removed_count > 0:
                logger.info(f"Cleanup: {removed_count} expired message(s) removed")
    
    def get_stats(self) -> Dict:
        """Get queue statistics"""
        with self.lock:
            total_messages = sum(len(msgs) for msgs in self.messages.values())
            return {
                "total_messages": total_messages,
                "ea_count": len(self.messages),
                "queues": {ea: len(msgs) for ea, msgs in self.messages.items()}
            }

# Global message queue instance
message_queue = MessageQueue()

# ============================================================================
# Background Cleanup Task
# ============================================================================

def start_cleanup_task():
    """Background task to cleanup expired messages"""
    def cleanup_loop():
        import time
        while True:
            time.sleep(CLEANUP_INTERVAL_SECONDS)
            try:
                message_queue.cleanup_expired()
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
    
    thread = threading.Thread(target=cleanup_loop, daemon=True)
    thread.start()
    logger.info(f"Cleanup task started (interval: {CLEANUP_INTERVAL_SECONDS}s)")

# ============================================================================
# API Endpoints
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Initialize background tasks on startup"""
    start_cleanup_task()
    logger.info("MT4 Message Broker started successfully")

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "online",
        "service": "MT4 Message Broker",
        "version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health")
async def health_check():
    """Detailed health check with statistics"""
    stats = message_queue.get_stats()
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "statistics": stats
    }

@app.post("/send", response_model=MessageResponse)
async def send_message(message: Message):
    """
    Send a message from one EA to another
    
    The message will be queued and delivered once to the target EA
    Messages expire after MESSAGE_TTL_SECONDS if not retrieved
    """
    try:
        # Validate required fields
        if not message.from_ea or not message.to_ea:
            raise HTTPException(status_code=400, detail="from_ea and to_ea are required")
        
        if not message.type:
            raise HTTPException(status_code=400, detail="type is required")
        
        # Add message to queue
        message_id = message_queue.add_message(message)
        
        return MessageResponse(
            status="success",
            message_id=message_id,
            timestamp=datetime.utcnow().isoformat()
        )
    
    except Exception as e:
        logger.error(f"Error sending message: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/poll/{ea_name}", response_model=MessagesResponse)
async def poll_messages(ea_name: str):
    """
    Poll for messages addressed to a specific EA
    
    Returns all queued messages and removes them (one-time delivery)
    Call this periodically from OnTimer in your MT4 EA
    """
    try:
        if not ea_name:
            raise HTTPException(status_code=400, detail="ea_name is required")
        
        # Get and remove messages (one-time delivery)
        messages = message_queue.get_messages(ea_name)
        
        return MessagesResponse(
            status="success",
            count=len(messages),
            messages=messages
        )
    
    except Exception as e:
        logger.error(f"Error polling messages: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_statistics():
    """Get current queue statistics"""
    return message_queue.get_stats()

# ============================================================================
# Error Handlers
# ============================================================================

@app.exception_handler(404)
async def not_found_handler(request, exc):
    return {
        "status": "error",
        "detail": "Endpoint not found",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    logger.error(f"Internal error: {exc}")
    return {
        "status": "error",
        "detail": "Internal server error",
        "timestamp": datetime.utcnow().isoformat()
    }

# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    import uvicorn

    print("=" * 60)
    print("MT4 Message Broker Starting...")
    print("=" * 60)
    print(f"Message TTL: {MESSAGE_TTL_SECONDS} seconds")
    print(f"Cleanup Interval: {CLEANUP_INTERVAL_SECONDS} seconds")
    print("=" * 60)

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8000)),
        log_level="info"
    )


