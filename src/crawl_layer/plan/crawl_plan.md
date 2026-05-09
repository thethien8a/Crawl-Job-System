# Crawl Layer Build Plan (Lakehouse-Lite)

## 1. Objective
Build a high-performance crawl layer (`crawl_layer`) to extract data from 4 sources: TopCV, ITviec, VietnamWorks, and LinkedIn. Run 4 concurrent threads/processes corresponding to 4 separate scripts and optimize scraping speed for each source.

## 2. Dynamic Website Mechanism Solution (ITviec, VietnamWorks)
For dynamic (Client-Side Rendered) websites, there are 2 approaches:
- **Reverse Engineer Internal API (Priority #1):** These websites use JS frameworks (React, Vue,...) to render the UI; data is loaded in the background via APIs (XHR/Fetch) in JSON format. We will use DevTools Network to analyze, discover hidden API endpoints, then simulate requests with Headers/Tokens. This approach delivers maximum speed, saves resources, and returns clean data (JSON).
- **Headless Browser (Fallback):** Use Playwright / Selenium to control a headless browser. Only use this if the website has aggressive anti-bot mechanisms (Cloudflare, etc.) that block direct API calls. The downside is that it is slow and consumes RAM/CPU.

## 3. Concurrency Architecture (Hybrid Architecture)
The system will combine Multiprocessing and Asyncio to achieve the highest performance and stability:

*   **Orchestrator Layer - Multiprocessing:**
    Use `multiprocessing` (such as `ProcessPoolExecutor`) to launch 4 scripts (TopCV, ITviec, VietnamWorks, LinkedIn) on 4 independent processes.
    *   *Fault Isolation:* A failure in one script (e.g., LinkedIn consuming too much memory or crashing) will not interrupt the other running scripts.
    *   *CPU Optimization:* Avoids Python's Global Interpreter Lock (GIL) bottleneck for computation-heavy tasks such as HTML parsing or IPC communication with a virtual browser.
    *   *Scalability:* Ready to be split into separate microservices or cron jobs on Docker/Kubernetes in the future.
*   **Script Layer (Per Crawler) - Asyncio:**
    Inside each process, use `asyncio` combined with `aiohttp` to fire off dozens/hundreds of concurrent requests to handle network I/O non-linearly, instead of waiting for each synchronous request.

## 4. Implementation Steps (Roadmap)
1.  **Setup:** Initialize project structure: `topcv.py`, `itviec.py`, `vnwork.py`, `linkedin.py`, and `main.py` (process orchestrator file).
2.  **TopCV Crawler:** Write a direct API call script combining `asyncio` and `aiohttp`.
3.  **API Survey & Analysis:** Reverse engineer hidden APIs on ITviec and VietnamWorks to obtain request parameters and Tokens (if any).
4.  **ITviec & VietnamWorks Crawlers:** Implement crawler logic to fetch raw JSON directly, using `asyncio`.
5.  **Orchestrator Integration:** Build `main.py` using `ProcessPoolExecutor` to wire the scripts together, allowing 4 processes to start simultaneously.
6.  **LinkedIn Handling (Phase 2):** Special approach using Playwright + Stealth or purchasing cookies/proxy because LinkedIn has very complex scraper blocking mechanisms.