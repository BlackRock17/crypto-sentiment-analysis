from src.data_processing.database import get_db
from src.data_processing.crud import create_solana_token


def test_create_solana_token():
    db = next(get_db())

    # Test creating a token with unique address
    token = create_solana_token(
        db=db,
        token_address="So11111111111111111111111111111111111111113",  # Changed address
        symbol="USDC",
        name="USD Coin"
    )

    # Verify token was created
    assert token.token_address == "So11111111111111111111111111111111111111113"
    assert token.symbol == "USDC"
    assert token.name == "USD Coin"

    print("✓ Successfully created and verified Solana token")