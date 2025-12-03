# Email Enricher API

> Repository: `alhamdulillah_sass_emil_enricher`

Minimal Express service that generates candidate email addresses for a list of contacts, verifies them with MailTester Ninja, and returns the best match per contact.

## Features
- POST endpoint at `/v1/scraper/enricher/start` that accepts a batch of contacts
- Deterministic email pattern generator covering common naming conventions
- MailTester Ninja client with key-provider integration and throttled requests
- Catch-all handling rules to surface the most useful fallback when no address validates
- Centralized error handling and JSON-only responses

## Getting Started
1. **Install dependencies**
   ```bash
   npm install
   ```
2. **Set environment variables** (see below). You can use a process manager, `.env`, or your shell profile.
3. **Run the server**
   ```bash
   npm start
   ```
   The server listens on `PORT` (defaults to `3000`).

## Configuration
| Variable | Default | Purpose |
| --- | --- | --- |
| `MAILTESTER_BASE_URL` | `https://happy.mailtester.ninja/ninja` | MailTester Ninja endpoint used for validation |
| `KEY_PROVIDER_URL` | `https://api.daddy-leads.com/mailtester/key/available` | Internal service that returns a MailTester key |
| `MIN_DELAY_MS` | `900` | Minimum ms between outbound MailTester requests (basic rate limiting) |
| `PORT` | `3000` | HTTP port for the Express server |

## API Usage
**Endpoint**: `POST /v1/scraper/enricher/start`

**Request body**
```json
{
  "contacts": [
    {
      "firstName": "Ada",
      "lastName": "Lovelace",
      "domain": "example.com"
    }
  ]
}
```

**Response body**
```json
{
  "results": [
    {
      "firstName": "Ada",
      "lastName": "Lovelace",
      "domain": "example.com",
      "bestEmail": "ada.lovelace@example.com",
      "status": "valid",
      "details": {
        "code": "ok",
        "message": "deliverable"
      },
      "allCheckedCandidates": [
        {
          "email": "ada.lovelace@example.com",
          "code": "ok",
          "message": "deliverable"
        }
      ]
    }
  ]
}
```

**cURL example**
```bash
curl -X POST http://localhost:3000/v1/scraper/enricher/start \
  -H "Content-Type: application/json" \
  -d '{
    "contacts": [
      {"firstName": "Ada", "lastName": "Lovelace", "domain": "example.com"}
    ]
  }'
```

## Project Structure
```
src/
  config/       # env + runtime configuration
  clients/      # MailTester + key provider integrations
  controllers/  # Express controllers
  routes/       # Express routers
  services/     # Enrichment logic
  utils/        # Helpers (patterns, rate limiter)
  server.js     # Express bootstrap + listener
```

## Notes
- API key fetching is cached per process to avoid overloading the provider.
- Rate limiting is coarse but ensures a minimum spacing between MailTester calls; adjust `MIN_DELAY_MS` to match your plan.
