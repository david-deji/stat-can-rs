use rand::{Rng, CryptoRng};
use sha2::{Sha256, Digest};
use hex;

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
}
