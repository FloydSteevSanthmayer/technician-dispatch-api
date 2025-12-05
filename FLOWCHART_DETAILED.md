Detailed step-by-step flow for technical reviewers

1. Receive POST /dispatch/{cust_id}.
2. Validate customer exists in `public.customers`. If not, return 404.
3. Fetch active technicians from `public.technicians`.
4. Compute haversine distance to each tech and shortlist top-K candidates.
5. Use OpenRouteService endpoints to compute driving distances in parallel for shortlist.
6. Select the technician with minimum driving distance.
7. Insert an assignment into `public.assignments` with distance and timestamp.
8. Return the assignment resource.

Additional notes:
- DB pools are created with asyncpg.create_pool.
- ORS calls are async and retry on transient failures (tenacity).
- Shortlist size is tunable (TOP_K variable).
- Consider using ORS Matrix endpoint for large-scale distance computations.
