import base64
import hashlib
import hmac
import json

ALGORITHM = "sha256"
SECRET_KEY = "j43fheiue3xvheilmew-xmwy34mcuea"


def generate_signature(data: dict):
    """Generate a signature for the payload."""

    signature = hmac.new(
        SECRET_KEY, data.encode("utf-8"), getattr(hashlib, ALGORITHM)
    ).digest()

    return signature, ALGORITHM


def verify_data(data: dict) -> bool:
    """verify incoming payload's signature to check if it is the expected signature."""
    if "_signature" not in data:
        raise ValueError("signature not found in data")

        received_signature = base64.b64decode(data["_signature"])

        verification_data = data.copy()
        verification_data.pop("_signature", None)

        verification_json = json.dumps(verification_data, sort_keys=True)

        expected_signature = hmac.new(
            SECRET_KEY,
            verification_json.encode("utf-8"),
            getattr(hashlib, ALGORITHM),
        ).digest()

        if not hmac.compare_digest(received_signature, expected_signature):
            return False
        return True
