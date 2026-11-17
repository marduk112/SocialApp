# SocialApp — Polyglot Persistence Social Media Service

[![CI](https://github.com/marduk112/SocialApp/actions/workflows/ci.yml/badge.svg)](https://github.com/marduk112/SocialApp/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A full-stack social network built to explore **hexagonal architecture, DDD and event-driven design** with polyglot persistence:
users and posts in PostgreSQL, the follow graph in Neo4j, and Kafka between bounded contexts.

## Highlights

- **Bounded contexts that never import each other** (UserManagement, SocialGraph, Feed, Notification): cross-context communication is Kafka only.
- **Transactional outbox** for every domain event (Postgres), plus a Neo4j-side outbox imported into it: no lost or phantom events, order kept per key.
- **Live updates over SSE**: a real-time feed and per-user notifications.
- **Discovery**: friends-of-friends "who to follow" (Cypher), full-text post search (`tsvector`), people search (`pg_trgm`), trending hashtags.
- **Posts**: hashtags and `@mentions`, reposts and quotes, images (validated and stripped of metadata), bookmarks, keyset pagination.
- **Current stack**: Java 25, Spring Boot 4.1, Angular 22 (zoneless, signals), PostgreSQL 18, Neo4j 5.26 LTS, Kafka 8.3.
- **Tested end to end**: ~570 backend tests (Testcontainers against real Postgres, Neo4j and Kafka) and 85 Playwright e2e tests.

## Stack
| Layer | Technology |
|---|---|
| Backend | Java 25, Spring Boot 4.1, Hexagonal Architecture |
| Relational DB | PostgreSQL 18 (UserManagement, Feed) |
| Graph DB | Neo4j 5.26 LTS + APOC (SocialGraph) |
| Messaging | Apache Kafka (Confluent cp-kafka 8.3) — KRaft mode (no Zookeeper) |
| Real-time | Server-Sent Events (SSE via Spring MVC SseEmitter) |
| Frontend | Angular 22, Signals, Angular Material |

---

## Repositories

The project is split into three repositories that live side by side in one folder:

| Repository | Contents | Path |
|---|---|---|
| [`SocialApp`](https://github.com/marduk112/SocialApp) (this one) | `docker-compose.yml`, CI, documentation | `./` |
| [`socialapp-api`](https://github.com/marduk112/socialapp-api) | Spring Boot backend | `./backend` |
| [`socialapp-web`](https://github.com/marduk112/socialapp-web) | Angular frontend | `./frontend/social-app` |

```bash
git clone https://github.com/marduk112/SocialApp.git && cd SocialApp
git clone https://github.com/marduk112/socialapp-api.git backend
git clone https://github.com/marduk112/socialapp-web.git frontend/social-app
```

---

## Quick Start

### 1. Start infrastructure
```bash
docker compose up -d
```

| Service | URL |
|---|---|
| PostgreSQL | `localhost:5432` |
| Neo4j Browser | http://localhost:7474 (neo4j / socialapp123) |
| Kafka (external) | `localhost:29092` |
| Kafka UI | http://localhost:8090/kafka-ui |

### 2. Run backend
```bash
cd backend
./mvnw spring-boot:run
# API available at http://localhost:8080
```

### 3. Run frontend (main app)
```bash
cd frontend/social-app
npm install
ng serve
# App at http://localhost:4200
```

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     Modular Monolith (Spring Boot)              │
│                                                                  │
│  ┌──────────────────────┐       ┌──────────────────────────┐   │
│  │  UserManagement BC   │       │    SocialGraph BC        │   │
│  │  ─────────────────   │       │  ─────────────────────   │   │
│  │  User (AggregateRoot)│       │  UserNode (Neo4j Node)   │   │
│  │  UserService         │       │  SocialGraphService      │   │
│  │  PostgreSQL          │       │  Neo4j [:FOLLOWS]        │   │
│  └──────────┬───────────┘       └──────────▲───────────────┘   │
│             │  UserCreatedEvent             │                   │
│             │  UserUpdatedEvent             │                   │
│             └──────────────┐   ┌───────────┘                   │
│                            ▼   │                               │
│              ┌─────────────────────────┐                       │
│              │         Kafka           │                       │
│              │   user-events topic     │                       │
│              │   post-events topic     │                       │
│              │   reaction-events topic │                       │
│              └────────────┬────────────┘                       │
│                           │ PostCreatedEvent                   │
│             ┌─────────────▼──────────────┐                     │
│             │       Feed BC              │                     │
│             │  Post (AggregateRoot)      │                     │
│             │  PostService               │──── SSE ──► Browser│
│             │  FeedSseService            │                     │
│             │  PostgreSQL                │                     │
│             └────────────────────────────┘                     │
└─────────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

### Domain Events: `@DomainEvents` vs Outbox Pattern
We use Spring Data's `AbstractAggregateRoot` + `@TransactionalEventListener(AFTER_COMMIT)`:
- **Pro**: Simple, zero infrastructure overhead
- **Con**: If Kafka is down after DB commit → event lost
- **When to switch to Outbox**: Production systems requiring guaranteed delivery — use Debezium CDC on the outbox table

### Bounded Context isolation
Modules communicate **only** via Kafka events. The `SocialGraph` module has **no import** from `UserManagement` — it only knows about shared `DomainEvent` records.

### SSE Architecture
`FeedSseService` holds a `CopyOnWriteArrayList<SseEmitter>`. On `PostCreatedEvent` from Kafka the service broadcasts asynchronously to all connected clients. Dead emitters (disconnected clients) are pruned on each broadcast.

### Angular Signals
- `FeedComponent` — all state in `signal<Post[]>()`, `computed()` for derived feed merge
- `UserProfileComponent` — `computed()` maps domain `User` → `UserCardData` DTO
- No RxJS `.subscribe()` inside component class for state management

---

## API Reference

### Users
```
POST   /api/v1/users              — register user
GET    /api/v1/users/{id}         — get profile
PUT    /api/v1/users/{id}         — update profile
```

### Social Graph
```
POST   /api/v1/social/follow                   — follow user
DELETE /api/v1/social/follow                   — unfollow
GET    /api/v1/social/{userId}/following       — list following
GET    /api/v1/social/{userId}/followers       — list followers
GET    /api/v1/social/is-following?followerId=&followeeId=
```

### Feed & Posts
```
GET    /api/v1/feed               — paginated feed
GET    /api/v1/feed/stream        — SSE stream (text/event-stream)
POST   /api/v1/posts              — create post
GET    /api/v1/posts/{id}         — get post
POST   /api/v1/posts/{id}/like    — like post
DELETE /api/v1/posts/{id}/like    — remove like
POST   /api/v1/posts/{id}/dislike — dislike post
DELETE /api/v1/posts/{id}/dislike — remove dislike
```

---

## Running Integration Tests
```bash
cd backend
./mvnw test
# Testcontainers will pull and start PostgreSQL, Neo4j and Kafka in parallel
```
