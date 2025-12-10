package com.redis_cache;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
@Transactional
public class SearchService {

    private final SearchKeywordRepository searchKeywordRepository;
    private final StringRedisTemplate stringRedisTemplate;

    private static final String POPULAR_KEYWORDS_KEY = "popular_keywords";
    private static final String RECENT_KEYWORDS_KEY = "recent_keywords";

    @CacheEvict(cacheNames = "search", allEntries = true)
    public void processSearch(String keyword) {
        saveOrUpdateSearchKeyword(keyword);
        updateRealTimeRanking(keyword);
        updateRecentKeywords(keyword);
    }

    @Transactional
    @CacheEvict(cacheNames = "search", allEntries = true)
    public void processSearchBulk(Map<String, Long> increments, List<String> recent) {
        LocalDateTime now = LocalDateTime.now();

        List<SearchKeyword> existList = searchKeywordRepository.findAllByKeywordIn(increments.keySet());
        Map<String, SearchKeyword> existMap = existList.stream()
                .collect(Collectors.toMap(SearchKeyword::getKeyword, k -> k));

        List<SearchKeyword> toSave = new ArrayList<>();
        for (Map.Entry<String, Long> e : increments.entrySet()) {
            String kw = e.getKey();
            long delta = e.getValue();
            SearchKeyword sk = existMap.get(kw);
            if (sk == null) {
                sk = SearchKeyword.builder()
                        .keyword(kw)
                        .searchCount(delta)
                        .firstSearchedAt(now)
                        .lastSearchedAt(now)
                        .build();
            } else {
                sk.setSearchCount(sk.getSearchCount() + delta);
                sk.setLastSearchedAt(now);
                if (sk.getFirstSearchedAt() == null) sk.setFirstSearchedAt(now);
            }
            toSave.add(sk);
        }
        if (!toSave.isEmpty()) {
            searchKeywordRepository.saveAll(toSave);
        }

        stringRedisTemplate.executePipelined((RedisConnection conn) -> {
            var ser = stringRedisTemplate.getStringSerializer();
            byte[] zkey = ser.serialize(POPULAR_KEYWORDS_KEY);
            byte[] lkey = ser.serialize(RECENT_KEYWORDS_KEY);

            for (Map.Entry<String, Long> e : increments.entrySet()) {
                conn.zIncrBy(zkey, e.getValue(), ser.serialize(e.getKey()));
            }
            if (recent != null && !recent.isEmpty()) {
                for (String kw : recent) {
                    conn.lRem(lkey, 0, ser.serialize(kw));
                    conn.lPush(lkey, ser.serialize(kw));
                }
                conn.lTrim(lkey, 0, 9);
            }
            return null;
        });
    }

    public Map<String, List<String>> fastGenerateAndSnapshot(Map<String, Long> increments, List<String> recent, int limit) {
        updateRedisBulkOnly(increments, recent);
        CompletableFuture.runAsync(() -> upsertDbBulk(increments));
        Map<String, List<String>> snap = new HashMap<>();
        snap.put("popular", getPopularKeywordsRaw(limit));
        snap.put("recent", getRecentKeywordsRaw(limit));
        return snap;
    }

    private void upsertDbBulk(Map<String, Long> increments) {
        LocalDateTime now = LocalDateTime.now();
        List<SearchKeyword> existList = searchKeywordRepository.findAllByKeywordIn(increments.keySet());
        Map<String, SearchKeyword> existMap = existList.stream()
                .collect(Collectors.toMap(SearchKeyword::getKeyword, k -> k));
        List<SearchKeyword> toSave = new ArrayList<>();
        for (Map.Entry<String, Long> e : increments.entrySet()) {
            String kw = e.getKey();
            long delta = e.getValue();
            SearchKeyword sk = existMap.get(kw);
            if (sk == null) {
                sk = SearchKeyword.builder()
                        .keyword(kw)
                        .searchCount(delta)
                        .firstSearchedAt(now)
                        .lastSearchedAt(now)
                        .build();
            } else {
                sk.setSearchCount(sk.getSearchCount() + delta);
                sk.setLastSearchedAt(now);
                if (sk.getFirstSearchedAt() == null) sk.setFirstSearchedAt(now);
            }
            toSave.add(sk);
        }
        if (!toSave.isEmpty()) {
            searchKeywordRepository.saveAll(toSave);
        }
    }

    private void updateRedisBulkOnly(Map<String, Long> increments, List<String> recent) {
        stringRedisTemplate.executePipelined((RedisConnection conn) -> {
            var ser = stringRedisTemplate.getStringSerializer();
            byte[] zkey = ser.serialize(POPULAR_KEYWORDS_KEY);
            byte[] lkey = ser.serialize(RECENT_KEYWORDS_KEY);

            for (Map.Entry<String, Long> e : increments.entrySet()) {
                conn.zIncrBy(zkey, e.getValue(), ser.serialize(e.getKey()));
            }
            if (recent != null && !recent.isEmpty()) {
                for (String kw : recent) {
                    conn.lRem(lkey, 0, ser.serialize(kw));
                    conn.lPush(lkey, ser.serialize(kw));
                }
                conn.lTrim(lkey, 0, 9);
            }
            return null;
        });
    }

    private void saveOrUpdateSearchKeyword(String keyword) {
        SearchKeyword searchKeyword = searchKeywordRepository
                .findByKeyword(keyword)
                .orElse(SearchKeyword.builder()
                        .keyword(keyword)
                        .searchCount(0L)
                        .build());
        searchKeyword.incrementSearchCount();
        searchKeywordRepository.save(searchKeyword);
    }

    private void updateRealTimeRanking(String keyword) {
        stringRedisTemplate.opsForZSet().incrementScore(POPULAR_KEYWORDS_KEY, keyword, 1);
    }

    private void updateRecentKeywords(String keyword) {
        stringRedisTemplate.opsForList().remove(RECENT_KEYWORDS_KEY, 0, keyword);
        stringRedisTemplate.opsForList().leftPush(RECENT_KEYWORDS_KEY, keyword);
        stringRedisTemplate.opsForList().trim(RECENT_KEYWORDS_KEY, 0, 9);
    }

    @Cacheable(value = "search", key = "'popular_keywords'")
    public List<String> getPopularKeywords(int limit) {
        try {
            Set<String> keywords = stringRedisTemplate.opsForZSet().reverseRange(POPULAR_KEYWORDS_KEY, 0, limit - 1);
            if (keywords == null) return List.of();
            return new ArrayList<>(keywords);
        } catch (RuntimeException ex) {
            safePurgeCorrupted();
            return List.of();
        }
    }

    @Cacheable(value = "search", key = "'recent_keywords'")
    public List<String> getRecentKeywords(int limit) {
        try {
            List<String> keywords = stringRedisTemplate.opsForList().range(RECENT_KEYWORDS_KEY, 0, limit - 1);
            if (keywords == null) return List.of();
            return keywords;
        } catch (RuntimeException ex) {
            safePurgeCorrupted();
            return List.of();
        }
    }

    public List<String> getPopularKeywordsRaw(int limit) {
        try {
            Set<String> keywords = stringRedisTemplate.opsForZSet().reverseRange(POPULAR_KEYWORDS_KEY, 0, limit - 1);
            if (keywords == null) return List.of();
            return new ArrayList<>(keywords);
        } catch (RuntimeException ex) {
            safePurgeCorrupted();
            return List.of();
        }
    }

    public List<String> getRecentKeywordsRaw(int limit) {
        try {
            List<String> keywords = stringRedisTemplate.opsForList().range(RECENT_KEYWORDS_KEY, 0, limit - 1);
            if (keywords == null) return List.of();
            return keywords;
        } catch (RuntimeException ex) {
            safePurgeCorrupted();
            return List.of();
        }
    }

    public List<String> getPopularKeywordsFromDB(int limit) {
        return searchKeywordRepository.findTop10ByOrderBySearchCountDesc()
                .stream().limit(limit).map(SearchKeyword::getKeyword).collect(Collectors.toList());
    }

    public List<String> getRecentKeywordsFromDB(int limit) {
        return searchKeywordRepository.findTop10ByOrderByLastSearchedAtDesc()
                .stream().limit(limit).map(SearchKeyword::getKeyword).collect(Collectors.toList());
    }

    public Map<String, Object> compareRedisVsDB() {
        long startTime, endTime;
        List<String> redisResult, dbResult;

        startTime = System.currentTimeMillis();
        redisResult = getPopularKeywordsRaw(10);
        endTime = System.currentTimeMillis();
        long redisTime = endTime - startTime;

        startTime = System.currentTimeMillis();
        dbResult = getPopularKeywordsFromDB(10);
        endTime = System.currentTimeMillis();
        long dbTime = endTime - startTime;

        return Map.of(
                "redisResult", redisResult,
                "dbResult", dbResult,
                "redisTime", redisTime + "ms",
                "dbTime", dbTime + "ms",
                "performanceImprovement", String.format("%.2f배", (double) dbTime / Math.max(redisTime, 1))
        );
    }

    @Cacheable(value = "search", key = "'autocomplete::' + #prefix")
    public List<String> getAutoCompleteKeywords(String prefix, int limit) {
        return searchKeywordRepository
                .findByKeywordStartingWithOrderBySearchCountDesc(prefix)
                .stream()
                .limit(limit)
                .map(SearchKeyword::getKeyword)
                .collect(Collectors.toList());
    }

    public SearchStatistics getSearchStatistics() {
        long totalKeywords = searchKeywordRepository.count();
        Long realtimeKeywordCount = stringRedisTemplate.opsForZSet().zCard(POPULAR_KEYWORDS_KEY);
        return SearchStatistics.builder()
                .totalKeywords(totalKeywords)
                .realtimeKeywordCount(realtimeKeywordCount != null ? realtimeKeywordCount : 0L)
                .lastUpdated(LocalDateTime.now())
                .build();
    }

    @lombok.Data
    @lombok.Builder
    public static class SearchStatistics {
        private Long totalKeywords;
        private Long realtimeKeywordCount;
        private LocalDateTime lastUpdated;
    }

    public void clearAllCacheFast() {
        stringRedisTemplate.delete(POPULAR_KEYWORDS_KEY);
        stringRedisTemplate.delete(RECENT_KEYWORDS_KEY);
    }

    public Map<String, Object> getRedisStatus() {
        ZSetOperations<String, String> zops = stringRedisTemplate.opsForZSet();
        Set<ZSetOperations.TypedTuple<String>> popularWithScores = zops.reverseRangeWithScores(POPULAR_KEYWORDS_KEY, 0, -1);
        List<String> recentKeywords = stringRedisTemplate.opsForList().range(RECENT_KEYWORDS_KEY, 0, -1);
        Long popularCount = zops.zCard(POPULAR_KEYWORDS_KEY);
        Long recentCount = stringRedisTemplate.opsForList().size(RECENT_KEYWORDS_KEY);
        return Map.of(
                "popularKeywords", popularWithScores != null ? popularWithScores : Set.of(),
                "recentKeywords", recentKeywords != null ? recentKeywords : List.of(),
                "totalPopularCount", popularCount != null ? popularCount : 0L,
                "totalRecentCount", recentCount != null ? recentCount : 0L
        );
    }

    private void safePurgeCorrupted() {
        try {
            stringRedisTemplate.delete(POPULAR_KEYWORDS_KEY);
            stringRedisTemplate.delete(RECENT_KEYWORDS_KEY);
        } catch (DataAccessException ignored) {
        }
    }
}
