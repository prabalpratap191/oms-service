package com.oms.service;

import com.oms.config.OmsProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

/** Redis in-flight lock + deterministic key = SHA-256(tenant + customer + cart_token + amount). DB unique index is the final guard. */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {
    private static final String PREFIX = "oms:idem:";
    private static final DefaultRedisScript<Long> UNLOCK = new DefaultRedisScript<>(
        "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end", Long.class);

    private final StringRedisTemplate redis;
    private final OmsProperties props;

    public static String key(String tenant, String customer, String cartToken, BigDecimal amount) {
        return sha256(tenant + "|" + customer + "|" + cartToken + "|" + amount.setScale(2, RoundingMode.HALF_UP).toPlainString());
    }

    public static String sha256(String s) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) { throw new IllegalStateException(e); }
    }

    /** @return lock token, or null when another request with the same key is in flight. */
    public String tryLock(String key) {
        String token = UUID.randomUUID().toString();
        Boolean ok = redis.opsForValue().setIfAbsent(PREFIX + key, token, Duration.ofSeconds(props.idempotency().lockTtlSeconds()));
        return Boolean.TRUE.equals(ok) ? token : null;
    }

    public void unlock(String key, String token) {
        try { redis.execute(UNLOCK, List.of(PREFIX + key), token); } catch (Exception ignored) { /* TTL will expire it */ }
    }
}
