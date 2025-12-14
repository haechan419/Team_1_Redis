# 🔍 실시간 검색어 시스템 (Redis + Spring Boot)

> **Redis를 활용한 고성능 실시간 검색어 순위 시스템**  
> 대규모 트래픽 환경에서도 빠르고 안정적인 실시간 검색어 서비스를 구현합니다.

---

## 🎯 핵심 특징

- ⚡ **빠른 응답속도**  
  Redis 인메모리 캐시 기반으로 밀리초(ms) 단위 응답

- 🔄 **Write-Back 패턴 적용**  
  Redis 우선 처리 + 비동기 DB 동기화로 DB 부하 최소화

- 📊 **실시간 순위 집계**  
  Sorted Set 기반 자동 정렬 및 실시간 인기 검색어 제공

- 🎨 **직관적인 UI**  
  실시간 검색어 시각화 및 Redis vs DB 성능 비교 대시보드 제공

---

## ✨ 주요 기능

### 1️⃣ 실시간 인기 검색어

- Redis **Sorted Set (ZSET)** 기반 순위 집계
- 검색 횟수(score) 자동 누적
- 상위 10개 인기 검색어 실시간 노출

### 2️⃣ 최근 검색어

- Redis **List** 기반 시간순 저장
- 최대 10개 유지 (자동 trimming)
- 중복 검색어 제거 및 최신 검색어 우선 유지

### 3️⃣ 성능 비교 대시보드

- Redis vs DB 조회 성능 실시간 비교
- 응답 시간(ms) 측정 및 시각화
- Redis Key-Value 구조 디버깅 기능 제공

---

## 🛠 기술 스택

### Backend
- Spring Boot 3.x
- Spring Data Redis
- Spring Data JPA
- Lombok

### Database & Cache
- Redis
- MySQL

### Frontend
- Vanilla JavaScript
- HTML5 / CSS3

### 📘 프로젝트 노션
🔗 [React Project Notion](https://www.notion.so/2c4e16d091798058b917c29192e59d66)


## 학습 목표

- Spring Boot와 Redis 연동 방법
- 캐시 전략과 성능 최적화 기법
- 실시간 데이터 처리 시스템 구축
- RESTful API 설계 및 구현


![구현화면](https://github.com/user-attachments/assets/bfe18443-79ce-4f31-b0dd-a4eb85c8f2b6)

## FlowChart

![플로우차트 수정 전](.images/Redis_전.png)

![플로우차트 수정 후](.images/Redis_수정후.png)

## 데이터 개선 작업
![플로우차트](.images/100만개 데이터 추가 후 개선 전 속도.png)

![플로우차트](.images/100만개 데이터 추가 후 개선 후 속도.png)


---

## 🔄 데이터 흐름

1. **검색 요청** → Redis에 즉시 반영
2. **조회 요청** → Redis에서 직접 조회
3. **비동기 동기화** → 스케줄러가 Redis → DB 주기적 반영

---

## 🔄 리팩토링: Write-Back 패턴 적용

### 주요 변경 사항

#### Redis 중심 구조
- 검색 시 Redis에 즉시 반영
- DB 저장은 10초 단위 비동기 처리
- 조회는 Redis 우선, 필요 시 DB 접근

#### 제거된 방식
- 3초마다 DB를 조회하는 폴링 방식 제거

#### 이벤트 기반 갱신
- 검색 버튼 클릭 시에만 갱신
- 불필요한 네트워크 요청 제거

---

## 📊 성능 최적화 결과

| 항목 | 개선 전 | 개선 후 | 개선율 |
|------|--------|--------|--------|
| 검색 응답 시간 | ~50ms | ~2ms | 96% 향상 |
| DB 쿼리 수 | 검색마다 1회 | 10초마다 1회 | 95% 감소 |
| 조회 응답 시간 | ~30ms | ~1ms | 97% 향상 |
| 네트워크 요청 | 3초 폴링 | 이벤트 기반 | 불필요 요청 제거 |

---

## 🗄 Redis 데이터 구조

### Key 설계

| Key | Type | 설명 |
|-----|------|------|
| `popular_keywords` | Sorted Set | 검색어별 검색 횟수 저장 |
| `recent_keywords` | List | 최근 검색어 시간순 저장 (최대 10개) |

# 📡 주요 API
## 🔍 검색 API

POST /api/search
검색어 처리

## 📈 조회 API

GET /api/search/popular
인기 검색어 조회

GET /api/search/recent
최근 검색어 조회

## 🐞 디버깅 API

GET /api/search/debug/redis-status
Redis 상태 확인

GET /api/search/compare/redis-vs-db
성능 비교

## 🧪 테스트 API

POST /api/test/generate-data
테스트 데이터 생성

POST /api/test/clear-cache
캐시 초기화

## 🚀 실행 방법

1️⃣ Redis 실행
# Docker 사용
docker start

# 또는 로컬 실행
redis-server


2️⃣ 애플리케이션 실행


3️⃣ 브라우저 접속
http://localhost:8080

# 🎓 학습 목표 정리
Redis 자료구조(Sorted Set, List) 실전 활용

Write-Back 패턴 구현

Cache-Aside 패턴 적용

DB 부하 분산 아키텍처 설계

비동기 처리 및 스케줄링

RESTful API 설계

# 📁 프로젝트 구조
```plaintext
src/
├── main/
│   ├── java/com/redis_cache/
│   │   ├── RedisCacheApplication.java
│   │   ├── CacheConfig.java
│   │   ├── SearchKeyword.java
│   │   ├── SearchKeywordRepository.java
│   │   ├── SearchService.java
│   │   ├── SearchController.java
│   │   └── TestDataController.java
│   │
│   └── resources/
│       ├── static/
│       │   ├── index.html
│       │   └── js/script.js
│       └── application.yml
```
