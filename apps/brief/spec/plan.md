# Implementation Plan: Brief Service

## Overview

Build `apps/brief` as a Kafka consumer/producer that wraps all LLM interactions.

## Architecture (High Level)

- Input: `summary.requests` (request includes window + top topics + evidence references)
- Evidence retrieval (MVP): none required; request includes bounded evidence excerpts
- LLM call: LangChain orchestration (provider/model configurable)
- Output: `summary.results` (`BriefResult`)
- Read model: Postgres (`brief_results`)

## Phases

### Phase 1: Message contracts + stub generator

- Implement `Brief` contract and a “no-LLM” deterministic brief for testing

### Phase 2: LLM integration + budgets

- Add provider integration
- Enforce budgets and max topics per brief

### Phase 3: Quality improvements

- Source diversity selection
- Deduping evidence
- Prompt tuning
