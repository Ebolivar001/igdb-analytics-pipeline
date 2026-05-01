# PUBG Analytics Pipeline Documentation

## 1. Data Discovery: The "Samples" Phase

### Overview
The pipeline begins by fetching a randomized collection of recent match references from the PUBG API's `/samples` endpoint. This stage serves as the discovery mechanism for the entire system.

### Why we use Samples first:
*   **Match ID Discovery:** The PUBG API is structured such that detailed data (Match Details and Telemetry) can only be accessed if you already possess a specific `match_id`. Since there is no global "list all matches" endpoint, the Samples endpoint provides the necessary "seeds" for data extraction.
*   **Unbiased Data Collection:** Unlike searching for specific players (which limits data to certain skill brackets or playstyles), the Samples endpoint returns a cross-section of recent activity across an entire platform shard (e.g., Steam). This is critical for building a representative and statistically valid analytics model.
*   **Pipeline Entry Point:** Every high-resolution data point we collect follows a strict dependency chain:
    1.  **Samples:** Discovers the `match_id`.
    2.  **Match Details:** Uses the ID to find the `telemetry_url`.
    3.  **Telemetry:** Downloads the actual event-level data (player movements, damage, kills).

### Extraction Workflow
1.  **Request:** Authenticate with the API and request the latest sample bundle.
2.  **Parse:** Extract the list of Match IDs from the JSON response.
3.  **Queue:** Pass these IDs to the next stage of the pipeline for detailed extraction.

---

## 2. Technical Architecture
The project is split into two primary methodologies:
*   **Python/Pandas (Extraction):** Used for lightweight API interaction, discovery, and initial telemetry fetching.
*   **PySpark (Transformation/Analysis):** Used for processing the high-volume telemetry data (often 30k+ rows per match) to ensure scalability.
