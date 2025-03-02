from fastapi import HTTPException, status


class BadRequestException(HTTPException):
    """Exception for 400 Bad Request errors"""
    def __init__(self, detail="Bad request"):
        super().__init__(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)