# SocialApp — LLM Instructions

## Project Overview

Full-stack social media application. **Polyglot persistence, hexagonal architecture, Event-Driven Architecture (EDA), DDD.**

- `backend/` — Spring Boot 4.1, Java 25, Maven
- `frontend/social-app/` — Angular 22 (standalone components, Angular Material, Signals)
- `docker-compose.yml` — PostgreSQL 18, Neo4j 5.26 LTS, Kafka 8.3 (Confluent cp-kafka, KRaft), Kafka UI (pinned v0.7.2)

## Running the stack

```bash
# Start infrastructure
docker compose up -d

# Backend (port 8080)
cd backend && ./mvnw spring-boot:run

# Frontend (port 4200)
cd frontend/social-app && npm start
```

Infrastructure ports:
- PostgreSQL: `localhost:5432`
- Neo4j Browser: `localhost:7474` (bolt: `7687`)
- Kafka broker: `localhost:29092`
- Kafka UI: `localhost:8090/kafka-ui`

## Architecture — Bounded Contexts

| BC | Persistence | Kafka role |
|---|---|---|
| **UserManagement** | PostgreSQL (JPA/Flyway) | Producer: `user-events` |
| **SocialGraph** | Neo4j | Consumer: `user-events` |
| **Feed** | PostgreSQL (JPA/Flyway) | Producer: `post-events`, `reaction-events`, `comment-events`; Consumer: `post-events` → SSE, `user-events`, `social-events` |
| **Notification** | PostgreSQL (JPA/Flyway) | Consumer: `user-events` (actor profiles), `social-events`, `post-events`, `reaction-events`, `comment-events` → notifications + per-user SSE |

**Golden rule: BCs must NOT import from each other.** Cross-BC communication only via Kafka events.

## Domain Events

Events flow (transactional outbox): `Domain aggregate → registerEvent() → OutboxEventWriter (BEFORE_COMMIT, same DB transaction) → outbox_events → OutboxRelay → Kafka → Consumer`.
SocialGraph (Neo4j) records its events in a Neo4j-side outbox that is imported into the Postgres outbox. Details: `backend/CLAUDE.md`.

Events (sealed interface `DomainEvent`):
- `UserCreatedEvent` / `UserUpdatedEvent` → topic `user-events`
- `UserFollowedEvent` / `UserUnfollowedEvent` → topic `social-events` (Feed keeps a local `follows` projection)
- `PostCreatedEvent` → topic `post-events` → SSE broadcast
- `ReactionEvent` → topic `reaction-events` (post AND comment reactions since 3A: `target`, `targetOwnerId`, `targetSnippet`)
- `CommentCreatedEvent` → topic `comment-events` (key postId; post author + parent comment author for replies)
- `PostCreatedEvent` also carries `originalAuthorId`, `originalContent`, `mentionedUserIds` (3A)

## REST API Summary

```
POST   /api/v1/users              # register
POST   /api/v1/users/login        # login (username OR email + password)
GET    /api/v1/users/{id}
PUT    /api/v1/users/{id}

POST   /api/v1/social/follow
DELETE /api/v1/social/follow
GET    /api/v1/social/{userId}/following
GET    /api/v1/social/{userId}/followers
GET    /api/v1/social/suggestions?limit=5   # who to follow (4A): [{userId, username, avatarUrl, mutualFollowers}], limit clamped 1..20

GET    /api/v1/feed?scope=all|following&cursor=&size=   # keyset-paginated -> {items, nextCursor}
GET    /api/v1/feed/stream?scope=all|following          # SSE (text/event-stream); token via ?access_token=
GET    /api/v1/hashtags/{tag}/posts?cursor=&size=       # posts with a hashtag (send '#' as %23)
GET    /api/v1/search/posts?q=&cursor=&size=            # full-text post search (4B), same envelope as /feed; q 1..100 chars
GET    /api/v1/search/users?q=&limit=10                 # people by username substring (4B) -> [{id, username, bio, avatarUrl}]
GET    /api/v1/trends/hashtags?hours=24&limit=10        # trending hashtags (4C) -> [{tag, posts, authors}]; hours 1..168, limit 1..20 (clamped)
POST   /api/v1/media                  # multipart 'file' (jpeg/png/gif/webp by magic bytes, <= 5 MB, <= 8000x8000)
                                      #   -> 201 {id, url, width, height}, unattached; 400 empty | 413 too large | 415 not
                                      #   an image | 429 > 20 pending; served at GET /uploads/media/<uuid>.<ext> (public)
POST   /api/v1/posts                  # {content, imageUrl?, mediaIds?: uuid[] <= 4} -> PostResponse incl. media[]
PUT    /api/v1/posts/{id}
DELETE /api/v1/posts/{id}
POST   /api/v1/posts/{id}/repost      # 201 REPOST | 200 if already reposted (idempotent)
DELETE /api/v1/posts/{id}/repost      # 204 always ({id} = original or a repost item)
POST   /api/v1/posts/{id}/quote       # {comment} 1..500, mediaIds? -> 201 QUOTE
                                      # mediaIds: own unattached uploads; unknown/foreign 404, attached/dup/>4 400
POST   /api/v1/posts/{id}/share       # DEPRECATED alias: blank comment -> repost, else quote
POST   /api/v1/posts/{id}/like
DELETE /api/v1/posts/{id}/like
POST   /api/v1/posts/{id}/dislike
DELETE /api/v1/posts/{id}/dislike
PUT    /api/v1/posts/{id}/bookmark    # 204 idempotent | 404 unknown post (private)
DELETE /api/v1/posts/{id}/bookmark    # 204 always
GET    /api/v1/bookmarks?cursor=&size=   # caller's bookmarks, newest bookmarked first -> {items, nextCursor}; Post DTOs carry `bookmarked`
POST   /api/v1/posts/{postId}/comments
GET    /api/v1/posts/{postId}/comments
PUT    /api/v1/comments/{commentId}
DELETE /api/v1/comments/{commentId}

GET    /api/v1/notifications?cursor=&size=20       # own notifications, keyset -> {items, nextCursor}
GET    /api/v1/notifications/unread-count          # {count}
POST   /api/v1/notifications/read                  # {ids?: uuid[]}; missing/empty = all -> 204
GET    /api/v1/notifications/stream                # SSE: connected / notification; token via ?access_token=
```

## Known Pitfalls (from project history)

1. **Kafka deserialization** — always configure `ErrorHandlingDeserializer` for value deserializer in consumer factory. Raw `JacksonJsonDeserializer` (`JsonDeserializer` before Spring Boot 4) without it causes `SerializationException` that bypasses the error handler.

2. **Dual Transaction Managers** — project uses both JPA and Neo4j. `JpaTransactionManager` is `@Primary`. When calling Neo4j services use `@Transactional("neo4jTransactionManager")` explicitly. Missing this causes `NoSuchBeanDefinitionException: No bean named 'transactionManager'`.

3. **Angular injection context (NG0203)** — never call `inject()` outside of a constructor or field initializer. Do NOT call Angular Material services (like `_HighContrastModeDetector`) in lifecycle hooks or lazy contexts.

4. **SSE + paginated feed deduplication** — `FeedComponent.allPosts` uses `computed()` to merge live SSE posts with paginated feed. Filter by ID to prevent duplicates. Do not break this merge logic.

5. **Frontend auth redirect** — after login, use Angular Router `navigate(['/feed'])` explicitly. Do not rely on guard redirects alone after setting `CurrentUserService`.

## Credentials (dev only)

| Service | User | Password |
|---|---|---|
| PostgreSQL | `socialapp` | `socialapp` |
| Neo4j | `neo4j` | `socialapp123` |

## CI

`.github/workflows/ci.yml` (GitHub Actions, on push to main/master and PRs) runs three jobs:
- `backend`: `cd backend && ./mvnw -B verify` (Testcontainers needs Docker).
- `frontend`: `cd frontend/social-app && npm ci && npm run build`.
- `e2e`: `docker compose up -d --wait postgres neo4j kafka`, run the backend jar (needs `APP_JWT_SECRET`, >= 32 bytes), then `npm run e2e` in `frontend/social-app`.
The Playwright e2e needs infra + backend running locally too (see "Running the stack"); the dev server on :4200 is started by `playwright.config.ts`.
The project is three git repositories side by side: this root (`marduk112/SocialApp`: `docker-compose.yml`, `.github/`, docs), `backend/` (`marduk112/socialapp-api`) and `frontend/social-app/` (`marduk112/socialapp-web`); the root `.gitignore` excludes the other two. The workflow lives in the root repo and checks the other two out next to it (default branch), and it only runs on pushes/PRs of the ROOT repo: changes in the backend/frontend repos are not built by it.
