package com.redis_cache;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/search")
@RequiredArgsConstructor
public class SearchController {

    private final SearchService searchService;

    @PostMapping
    public ResponseEntity<Map<String, Object>> search(@RequestBody Map<String, String> request) {
        String keyword = request.get("keyword");

        if (keyword == null || keyword.trim().isEmpty()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "검색어를 입력해주세요"));
        }

        searchService.processSearch(keyword.trim());

        // 검색 완료 후 현재 Redis 상태 조회
        Map<String, Object> redisStatus = searchService.getRedisStatus();

        return ResponseEntity.ok(Map.of(
                "message", "검색이 완료되었습니다 (Redis Fast Write)",
                "keyword", keyword,
                "redisKeys", Map.of(
                        "popular_keywords", redisStatus.get("popularKeywords"),
                        "recent_keywords", redisStatus.get("recentKeywords")
                )
        ));
    }

    @GetMapping("/popular")
    public ResponseEntity<Map<String, Object>> getPopularKeywords() {
        Map<String, Object> redisStatus = searchService.getRedisStatus();
        return ResponseEntity.ok(Map.of(
                "keywords", searchService.getPopularKeywords(10),
                "redisKey", "popular_keywords",
                "redisValue", redisStatus.get("popularKeywords"),
                "totalCount", redisStatus.get("totalPopularCount")
        ));
    }

    @GetMapping("/recent")
    public ResponseEntity<Map<String, Object>> getRecentKeywords() {
        Map<String, Object> redisStatus = searchService.getRedisStatus();
        return ResponseEntity.ok(Map.of(
                "keywords", searchService.getRecentKeywords(10),
                "redisKey", "recent_keywords",
                "redisValue", redisStatus.get("recentKeywords"),
                "totalCount", redisStatus.get("totalRecentCount")
        ));
    }

    @GetMapping("/debug/redis-status")
    public ResponseEntity<Map<String, Object>> getRedisStatus() {
        Map<String, Object> status = searchService.getRedisStatus();
        return ResponseEntity.ok(Map.of(
                "redisData", Map.of(
                        "popular_keywords", Map.of(
                                "type", "SortedSet",
                                "value", status.get("popularKeywords"),
                                "count", status.get("totalPopularCount")
                        ),
                        "recent_keywords", Map.of(
                                "type", "List",
                                "value", status.get("recentKeywords"),
                                "count", status.get("totalRecentCount")
                        )
                ),
                "popularKeywords", status.get("popularKeywords"),
                "recentKeywords", status.get("recentKeywords"),
                "totalPopularCount", status.get("totalPopularCount"),
                "totalRecentCount", status.get("totalRecentCount")
        ));
    }

    @GetMapping("/debug/redis-keys")
    public ResponseEntity<Map<String, Object>> getAllRedisKeys() {
        Map<String, Object> status = searchService.getRedisStatus();
        return ResponseEntity.ok(Map.of(
                "keys", Map.of(
                        "popular_keywords", Map.of(
                                "dataType", "SortedSet (ZSET)",
                                "currentValue", status.get("popularKeywords")
                        ),
                        "recent_keywords", Map.of(
                                "dataType", "List",
                                "currentValue", status.get("recentKeywords")
                        )
                )
        ));
    }

    @GetMapping("/compare/redis-vs-db")
    public ResponseEntity<Map<String, Object>> compareRedisVsDB() {
        Map<String, Object> comparison = searchService.compareRedisVsDB();
        Map<String, Object> redisStatus = searchService.getRedisStatus();

        return ResponseEntity.ok(Map.of(
                "redisResult", comparison.get("redisResult"),
                "dbResult", comparison.get("dbResult"),
                "redisTime", comparison.get("redisTime"),
                "dbTime", comparison.get("dbTime"),
                "performanceImprovement", comparison.get("performanceImprovement"),
                "redisKeyValueData", Map.of(
                        "popular_keywords", redisStatus.get("popularKeywords"),
                        "recent_keywords", redisStatus.get("recentKeywords")
                )
        ));
    }
}