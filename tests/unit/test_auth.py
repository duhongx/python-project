from db_schema_sync_client.infrastructure.credentials import hash_password, verify_password


def test_hash_password_does_not_store_plaintext():
    hashed = hash_password("cloudhis@2123")

    assert hashed != "cloudhis@2123"
    assert hashed.startswith("pbkdf2_sha256$")


def test_verify_password_accepts_correct_secret():
    hashed = hash_password("cloudhis@2123")

    assert verify_password("cloudhis@2123", hashed) is True


def test_verify_password_rejects_wrong_secret():
    hashed = hash_password("cloudhis@2123")

    assert verify_password("wrong-password", hashed) is False
