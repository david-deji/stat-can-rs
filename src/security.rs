use rand::{Rng, CryptoRng};
use sha2::{Sha256, Digest};
use hex;
use constant_time_eq::constant_time_eq;

/// Validates an API key against a provided header value.
/// Supports "Bearer <token>" or direct token format.
/// Uses constant-time comparison to prevent timing attacks.
pub fn validate_api_key(expected_key: &str, received_header: Option<&str>) -> bool {
    if let Some(mut header_val) = received_header {
        // Strip "Bearer " prefix if present
        if header_val.starts_with("Bearer ") {
            header_val = &header_val[7..];
        }

        // Use constant time comparison
        constant_time_eq(expected_key.as_bytes(), header_val.as_bytes())
    } else {
        false
    }
}

/// Generates a new API key and its hash.
/// Returns (api_key, key_hash)
pub fn generate_api_key<R: Rng + CryptoRng>(rng: &mut R) -> (String, String) {
    let mut random_bytes = [0u8; 24];
    rng.fill(&mut random_bytes);
    let key_secret = hex::encode(random_bytes);
    let api_key = format!("sk_live_{}", key_secret);

    let mut hasher = Sha256::new();
    hasher.update(api_key.as_bytes());
    let key_hash = hex::encode(hasher.finalize());

    (api_key, key_hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn test_generate_api_key_deterministic() {
        // Seed for reproducibility
        let mut rng = StdRng::seed_from_u64(42);
        let (key, hash) = generate_api_key(&mut rng);

        // Verify format
        assert!(key.starts_with("sk_live_"));
        // 24 bytes = 48 hex chars. "sk_live_" is 8 chars. Total 56.
        assert_eq!(key.len(), 8 + 48);

        // Verify hash consistency
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        let expected_hash = hex::encode(hasher.finalize());
        assert_eq!(hash, expected_hash);
    }

    #[test]
    fn test_validate_api_key() {
        let key = "sk_live_123456";

        // Correct key
        assert!(validate_api_key(key, Some("sk_live_123456")));

        // Correct key with Bearer
        assert!(validate_api_key(key, Some("Bearer sk_live_123456")));

        // Incorrect key
        assert!(!validate_api_key(key, Some("sk_live_654321")));

        // Partial key (length mismatch)
        assert!(!validate_api_key(key, Some("sk_live_123")));

        // Longer key
        assert!(!validate_api_key(key, Some("sk_live_1234567")));

        // No header
        assert!(!validate_api_key(key, None));

        // Empty header
        assert!(!validate_api_key(key, Some("")));
    }
}
