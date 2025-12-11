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
import java.util.Random; // <-- 테스트 작성 신규 추가

@Service
@RequiredArgsConstructor
@Slf4j
public class SearchService {

    private final SearchKeywordRepository searchKeywordRepository;
    private final StringRedisTemplate stringRedisTemplate;

    private static final String POPULAR_KEYWORDS_KEY = "popular_keywords";
    private static final String RECENT_KEYWORDS_KEY = "recent_keywords";

    // =====================================================================
    // 1. [Cache Warming] 서버 시작 시 DB -> Redis 데이터 로딩
    // =====================================================================
    @PostConstruct
    public void init() {
        log.info("🚀 서버 시작: DB의 인기 검색어를 Redis로 로딩합니다...");

        List<SearchKeyword> topKeywords = searchKeywordRepository.findTop100ByOrderBySearchCountDesc();

        if (topKeywords != null && !topKeywords.isEmpty()) {
            Set<ZSetOperations.TypedTuple<String>> tuples = new HashSet<>();
            for (SearchKeyword kw : topKeywords) {
                tuples.add(ZSetOperations.TypedTuple.of(kw.getKeyword(), (double) kw.getSearchCount()));
            }
            stringRedisTemplate.opsForZSet().add(POPULAR_KEYWORDS_KEY, tuples);
            log.info("✅ Cache Warming 완료: {}개 키워드 로딩됨", tuples.size());
        }
    }

    // =====================================================================
    // 2. 검색 요청 처리 (Redis만 업데이트 -> 속도 매우 빠름)
    // =====================================================================
    public void processSearch(String keyword) {
        if (keyword == null || keyword.isBlank()) return;

        // 1. 인기 검색어 점수 증가 (DB 저장 안 함)
        stringRedisTemplate.opsForZSet().incrementScore(POPULAR_KEYWORDS_KEY, keyword, 1);

        // 2. 최근 검색어 업데이트
        stringRedisTemplate.opsForList().remove(RECENT_KEYWORDS_KEY, 0, keyword);
        stringRedisTemplate.opsForList().leftPush(RECENT_KEYWORDS_KEY, keyword);
        stringRedisTemplate.opsForList().trim(RECENT_KEYWORDS_KEY, 0, 9);
    }

    // =====================================================================
    // 3. 주기적 동기화 (Redis -> DB 반영, 10초 간격)
    // =====================================================================
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

    // =====================================================================
    // 4. 조회 및 유틸리티 메서드
    // =====================================================================
    public List<String> getPopularKeywords(int limit) {
        Set<String> keywords = stringRedisTemplate.opsForZSet().reverseRange(POPULAR_KEYWORDS_KEY, 0, limit - 1);
        return keywords == null ? List.of() : new ArrayList<>(keywords);
    }

    public List<String> getRecentKeywords(int limit) {
        List<String> keywords = stringRedisTemplate.opsForList().range(RECENT_KEYWORDS_KEY, 0, limit - 1);
        return keywords == null ? List.of() : keywords;
    }

    // 테스트 데이터 생성용 (즉시 DB 반영 포함)
    public Map<String, List<String>> fastGenerateAndSnapshot(Map<String, Long> increments, List<String> recent, int limit) {
        for (Map.Entry<String, Long> entry : increments.entrySet()) {
            stringRedisTemplate.opsForZSet().incrementScore(POPULAR_KEYWORDS_KEY, entry.getKey(), entry.getValue());
        }
        if (recent != null) {
            for(String r : recent) {
                stringRedisTemplate.opsForList().leftPush(RECENT_KEYWORDS_KEY, r);
            }
            stringRedisTemplate.opsForList().trim(RECENT_KEYWORDS_KEY, 0, 9);
        }

        syncToDatabase(); // 테스트 확인을 위해 강제 동기화 수행
        return Map.of("popular", getPopularKeywords(limit), "recent", getRecentKeywords(limit));
    }

    public void clearAllCacheFast() {
        stringRedisTemplate.delete(List.of(POPULAR_KEYWORDS_KEY, RECENT_KEYWORDS_KEY));
    }

    public Map<String, Object> getRedisStatus() {
        ZSetOperations<String, String> zops = stringRedisTemplate.opsForZSet();
        Set<ZSetOperations.TypedTuple<String>> popularWithScores = zops.reverseRangeWithScores(POPULAR_KEYWORDS_KEY, 0, -1);
        List<String> recentKeywords = stringRedisTemplate.opsForList().range(RECENT_KEYWORDS_KEY, 0, -1);

        return Map.of(
                "popularKeywords", popularWithScores != null ? popularWithScores : Set.of(),
                "recentKeywords", recentKeywords != null ? recentKeywords : List.of(),
                "totalPopularCount", Optional.ofNullable(zops.zCard(POPULAR_KEYWORDS_KEY)).orElse(0L),
                "totalRecentCount", Optional.ofNullable(stringRedisTemplate.opsForList().size(RECENT_KEYWORDS_KEY)).orElse(0L)
        );
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
    // 12.11 [신규 추가] 대량 테스트 데이터 생성 (1만 건)======================================================
    // 12.11 [수정됨] 대량 테스트 데이터 생성 서버 실행시 초기화 작업완료 (1만 건)
    public void generateBulkData(int count) {
        log.info("🔥 대량 데이터 생성 시작 ({}건)", count);

        // 1. Redis 캐시 초기화
        clearAllCacheFast();

        // [필수 추가] DB에 있는 기존 데이터도 삭제해야 중복 에러가 안 납니다!
        // deleteAllInBatch()는 데이터를 하나씩 지우지 않고 통째로 날려서 속도가 빠릅니다.
        searchKeywordRepository.deleteAllInBatch();

        List<SearchKeyword> bulkData = new ArrayList<>();
        Set<ZSetOperations.TypedTuple<String>> redisData = new HashSet<>();
        LocalDateTime now = LocalDateTime.now();
        Random random = new Random();

        // 2. 데이터 생성 루프
        for (int i = 1; i <= count; i++) {
            String keyword;
            long searchCount;

            // 상위 1%는 "인기 키워드"로 설정
            if (i <= count * 0.01) {
                keyword = "인기검색어_" + i;
                searchCount = random.nextInt(100000) + 1000;
            } else {
                // 나머지는 일반 키워드
                keyword = "테스트_" + i;
                searchCount = random.nextInt(100) + 1;
            }

            // DB 엔티티 생성
            bulkData.add(SearchKeyword.builder()
                    .keyword(keyword)
                    .searchCount(searchCount)
                    .firstSearchedAt(now)
                    .lastSearchedAt(now)
                    .build());

            // Redis ZSet 데이터 생성
            redisData.add(ZSetOperations.TypedTuple.of(keyword, (double) searchCount));
        }

        // 3. DB에 한 방에 저장
        searchKeywordRepository.saveAll(bulkData);
        log.info("✅ DB 저장 완료");

        // 4. Redis에 한 방에 저장
        stringRedisTemplate.opsForZSet().add(POPULAR_KEYWORDS_KEY, redisData);

        // 최근 검색어 채우기
        for(int i=0; i<10; i++) {
            stringRedisTemplate.opsForList().leftPush(RECENT_KEYWORDS_KEY, "테스트_" + i);
        }
        log.info("✅ Redis 저장 완료");
    } // 테스트 더미데이터 코드 마지막 지점==========================================================
}