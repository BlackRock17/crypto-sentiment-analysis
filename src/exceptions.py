from fastapi import HTTPException, status


class BadRequestException(HTTPException):
    """Exception for 400 Bad Request errors"""
    def __init__(self, detail="Bad request"):
        super().__init__(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)


class UnauthorizedException(HTTPException):
    """Exception for 401 Unauthorized errors"""
    def __init__(self, detail="Authentication required"):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=detail,
            headers={"WWW-Authenticate": "Bearer"}
        )


class ForbiddenException(HTTPException):
    """Exception for 403 Forbidden errors"""
    def __init__(self, detail="Insufficient permissions"):
        super().__init__(status_code=status.HTTP_403_FORBIDDEN, detail=detail)


class NotFoundException(HTTPException):
    """Exception for 404 Not Found errors"""
    def __init__(self, detail="Resource not found"):
        super().__init__(status_code=status.HTTP_404_NOT_FOUND, detail=detail)


class ConflictException(HTTPException):
    """Exception for 409 Conflict errors"""
    def __init__(self, detail="Resource conflict"):
        super().__init__(status_code=status.HTTP_409_CONFLICT, detail=detail)
