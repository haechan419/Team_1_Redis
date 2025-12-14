package com.redis_cache;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
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
    private static final String DIRTY_KEYWORDS_KEY = "dirty:keywords";

    // 서버 시작 시 DB-> Redis Cache
    @PostConstruct
    public void init() {
        log.info("🚀 서버 시작: DB의 인기 검색어를 Redis로 로딩합니다...");

        List<SearchKeyword> topKeywords = searchKeywordRepository.findTop10ByOrderBySearchCountDesc();

        if (topKeywords != null && !topKeywords.isEmpty()) {
            Set<ZSetOperations.TypedTuple<String>> tuples = new HashSet<>();
            for (SearchKeyword kw : topKeywords) {
                tuples.add(ZSetOperations.TypedTuple.of(kw.getKeyword(), (double) kw.getSearchCount()));
            }
            stringRedisTemplate.opsForZSet().add(POPULAR_KEYWORDS_KEY, tuples);
            log.info("✅ Cache Warming 완료: {}개 키워드 로딩됨", tuples.size());
        }
    }

    public void processSearch(String keyword) {
        if (keyword == null || keyword.isBlank()) return;

        // 1. 인기 검색어 점수 증가 (DB 저장 안 함)
        stringRedisTemplate.opsForZSet().incrementScore(POPULAR_KEYWORDS_KEY, keyword, 1);

        // 2. 최근 검색어 업데이트
        stringRedisTemplate.opsForList().remove(RECENT_KEYWORDS_KEY, 0, keyword);
        stringRedisTemplate.opsForList().leftPush(RECENT_KEYWORDS_KEY, keyword);
        stringRedisTemplate.opsForList().trim(RECENT_KEYWORDS_KEY, 0, 9);
    }

    @Scheduled(fixedDelay = 10000)
    @Transactional
    public void syncToDatabase() {
        // Redis에서 상위 100개 키워드와 점수 가져오기
        Set<ZSetOperations.TypedTuple<String>> tuples =
                stringRedisTemplate.opsForZSet().reverseRangeWithScores(POPULAR_KEYWORDS_KEY, 0, 99);

        if (tuples == null || tuples.isEmpty()) return;

        List<String> keywords = tuples.stream().map(ZSetOperations.TypedTuple::getValue).collect(Collectors.toList());
        List<SearchKeyword> existingKeywords = searchKeywordRepository.findAllByKeywordIn(keywords);

        Map<String, SearchKeyword> keywordMap = existingKeywords.stream()
                .collect(Collectors.toMap(SearchKeyword::getKeyword, k -> k));

        List<SearchKeyword> toSave = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();

        for (ZSetOperations.TypedTuple<String> tuple : tuples) {
            String kw = tuple.getValue();
            Double score = tuple.getScore();

            if (kw == null || score == null) continue;

            long redisCount = score.longValue();
            SearchKeyword sk = keywordMap.get(kw);

            if (sk == null) {
                // DB에 없으면 신규 생성
                sk = SearchKeyword.builder()
                        .keyword(kw)
                        .searchCount(redisCount)
                        .firstSearchedAt(now)
                        .lastSearchedAt(now)
                        .build();
            } else {
                // DB에 있으면 Redis 점수로 업데이트 (Redis가 항상 최신이므로)
                if (sk.getSearchCount() < redisCount) {
                    sk.setSearchCount(redisCount);
                    sk.setLastSearchedAt(now);
                }
            }
            toSave.add(sk);
        }

        if (!toSave.isEmpty()) {
            searchKeywordRepository.saveAll(toSave);
        }
    }


// 테스트 데이터 생성용 (즉시 DB 반영 포함)
    public Map<String, List<String>> fastGenerateAndSnapshot(Map<String, Long> increments, List<String> recent, int limit) {
        for (Map.Entry<String, Long> entry : increments.entrySet()) {
            stringRedisTemplate.opsForZSet().incrementScore(POPULAR_KEYWORDS_KEY, entry.getKey(), entry.getValue());
        }
        if (recent != null) {
            for (String r : recent) {
                stringRedisTemplate.opsForList().leftPush(RECENT_KEYWORDS_KEY, r);
            }
            stringRedisTemplate.opsForList().trim(RECENT_KEYWORDS_KEY, 0, 9);
        }

        syncToDatabase(); // 테스트 확인을 위해 강제 동기화 수행
        return Map.of("popular", getPopularKeywords(limit), "recent", getRecentKeywords(limit));
    }

    // 스케줄러: Redis 점수 → DB 저장 → 처리된 키 제거
    @Scheduled(fixedDelayString = "${app.sync.delay-ms:60000}")
    public void syncDirtyKeywordsToDb() {
        Set<String> dirtyKeywords = stringRedisTemplate.opsForSet().members(DIRTY_KEYWORDS_KEY);
        if (dirtyKeywords == null || dirtyKeywords.isEmpty()) return;

        try {
            LocalDateTime now = LocalDateTime.now();
            List<SearchKeyword> existing = searchKeywordRepository.findAllByKeywordIn(dirtyKeywords);
            Map<String, SearchKeyword> existMap = existing.stream()
                    .collect(Collectors.toMap(SearchKeyword::getKeyword, k -> k));

            List<SearchKeyword> toSave = new ArrayList<>();
            for (String kw : dirtyKeywords) {
                Double score = stringRedisTemplate.opsForZSet().score(POPULAR_KEYWORDS_KEY, kw);
                long count = score != null ? score.longValue() : 0L;
                SearchKeyword sk = existMap.get(kw);
                if (sk == null) {
                    sk = SearchKeyword.builder()
                            .keyword(kw)
                            .searchCount(count)
                            .firstSearchedAt(now)
                            .lastSearchedAt(now)
                            .build();
                } else {
                    sk.setSearchCount(count);
                    sk.setLastSearchedAt(now);
                    if (sk.getFirstSearchedAt() == null) sk.setFirstSearchedAt(now);
                }
                toSave.add(sk);
            }

            if (!toSave.isEmpty()) {
                searchKeywordRepository.saveAll(toSave);
            }
            stringRedisTemplate.opsForSet().remove(DIRTY_KEYWORDS_KEY, (Object[]) dirtyKeywords.toArray(new String[0]));
        } catch (RuntimeException ex) {
            log.warn("Failed to sync dirty keywords to DB", ex);
        }
    }



    public List<String> getRecentKeywords(int limit) {
        List<String> keywords = stringRedisTemplate.opsForList().range(RECENT_KEYWORDS_KEY, 0, limit - 1);
        return keywords == null ? List.of() : keywords;
    }

    public List<String> getPopularKeywords(int limit) {
        Set<String> keywords = stringRedisTemplate.opsForZSet().reverseRange(POPULAR_KEYWORDS_KEY, 0, limit - 1);
        return keywords == null ? List.of() : new ArrayList<>(keywords);
    }

    public Map<String, Object> compareRedisVsDB() {
        long start, end;

        start = System.currentTimeMillis();
        List<String> redisResult = getPopularKeywords(10);
        end = System.currentTimeMillis();
        long redisTime = end - start;

        start = System.currentTimeMillis();
        List<String> dbResult = searchKeywordRepository.findTop10ByOrderBySearchCountDesc()
                .stream().limit(10).map(SearchKeyword::getKeyword).collect(Collectors.toList());
        end = System.currentTimeMillis();
        long dbTime = end - start;

        return Map.of(
                "redisResult", redisResult,
                "dbResult", dbResult,
                "redisTime", redisTime + "ms",
                "dbTime", dbTime + "ms",
                "performanceImprovement", String.format("%.2f배", (double) dbTime / Math.max(redisTime, 1))
        );
    }

    public void clearAllCacheFast() {
        stringRedisTemplate.delete(List.of(POPULAR_KEYWORDS_KEY, RECENT_KEYWORDS_KEY));
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
                "totalPopularCount", Optional.ofNullable(zops.zCard(POPULAR_KEYWORDS_KEY)).orElse(0L),
                "totalRecentCount", Optional.ofNullable(stringRedisTemplate.opsForList().size(RECENT_KEYWORDS_KEY)).orElse(0L)
        );
    }
}
