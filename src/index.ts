import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

// Boston APIs
const PERMIT_API = "https://data.boston.gov/datastore/odata3.0/6ddcd912-32a0-43df-9908-63574f8c7e77/Building_Permits";
const SR_API = "https://data.boston.gov/datastore/odata3.0/9d7c2214-4709-478a-a2e8-fb2020a5bb94";


// Building Permit api 
const CKAN_HOST = 'https://data.boston.gov';
const BUILDING_RESOURCE_ID = '6ddcd912-32a0-43df-9908-63574f8c7e77';
const SERVICE_RESOURCE_ID = '9d7c2214-4709-478a-a2e8-fb2020a5bb94';
const FOOD_RESOURCE_ID = '4582bec6-2b4f-4f9e-bc55-cbaa73117f4c';
const CRIME_RESOURCE_ID ='b973d8cb-eeb2-4e7e-99da-c92938efc9c0';
const CHECKBOOK_RESOURCE_ID = '84dfc1af-28bd-4f17-804a-9cc0c09a237e';
const PARALLEL_REQUESTS = 7;
const PAGE_SIZE = 100;


// Create MCP server
const server = new McpServer({
  name: "boston-data",
  version: "1.0.0",
  capabilities: {
    tools: {},
    resources: {},
  },
});






function normalizeAddress(address: string) {
  return address
    .replace(/\./g, '')
    .replace(/\bSTREET\b/gi, 'ST')
    .replace(/\bSt\b/gi, 'ST')
    .replace(/\bST\b/gi, 'ST')
    .replace(/\bSt.\b/gi, 'ST')
    .replace(/\bAVENUE\b/gi, 'AV')
    .replace(/\bAve\b/gi, 'AV')
    .replace(/\bAvenue\b/gi, 'AV')
    .replace(/\bAV\b/gi, 'AV')
    .replace(/\bAVE\b/gi, 'AV')
    .toUpperCase()
    .trim();
}

// Helper to fetch a single chunk
async function fetchPermitChunk(address: string, offset: number) {
  const params = new URLSearchParams({
    resource_id: BUILDING_RESOURCE_ID,
    q: address,
    limit: PAGE_SIZE.toString(),
    offset: offset.toString(),
  });
  const url = `${CKAN_HOST}/api/3/action/datastore_search?${params.toString()}`;
  const response = await fetch(url, { headers: { 'Content-Type': 'application/json' } });
  if (!response.ok) throw new Error(`API Error: ${response.status}`);
  const data = await response.json();
  if (!data.success) throw new Error("API call unsuccessful.");
  return data.result.records;
}

// Fetch all candidate records in parallel
async function fetchAllPermitsParallel(address: string): Promise<any[]> {
  // First, get total count for this address via q (broad, not normalized)
  const params = new URLSearchParams({
    resource_id: BUILDING_RESOURCE_ID,
    q: address,
    limit: '1',
    offset: '0'
  });
  const url = `${CKAN_HOST}/api/3/action/datastore_search?${params.toString()}`;
  const initialRes = await fetch(url, { headers: { 'Content-Type': 'application/json' } });
  if (!initialRes.ok) throw new Error(`API Error: ${initialRes.status}`);
  const initialData = await initialRes.json();
  const total = initialData.result.total || 0;
  if (total === 0) return [];

  // Calculate all needed offsets
  const offsets: number[] = [];
  for (let i = 0; i < total; i += PAGE_SIZE) offsets.push(i);

  // Fetch all in parallel batches
  let allRecords: any[] = [];
  for (let i = 0; i < offsets.length; i += PARALLEL_REQUESTS) {
    const chunkOffsets = offsets.slice(i, i + PARALLEL_REQUESTS);
    const chunkResults = await Promise.all(
      chunkOffsets.map(offset => fetchPermitChunk(address, offset))
    );
    chunkResults.forEach(records => allRecords.push(...records));
  }
  return allRecords;
}

// Building Permits Tool
server.tool(
  "get_building_permits_fuzzy_parallel",
  "Fetch ALL building permits for an address (parallel, fuzzy match)",
   {
    address: z.string().describe("Street address to search for (e.g., 65 Commonwealth Ave)"),
  },
  async ({ address }) => {
    const normalizedAddress = normalizeAddress(address); 
    let allRecords: any[];
    try {
      allRecords = await fetchAllPermitsParallel(normalizedAddress);
    } catch (e) {
      return { content: [{ type: "text", text: `Failed to fetch permits: ${e}` }] };
    }

    if (!allRecords.length) {
      return { content: [{ type: "text", text: "No building permits found for that address." }] };
    }

    // Normalize & filter (strict + forgiving match)
    const searchNorm = normalizeAddress(address);
    const matches = allRecords.filter((p: any) => {
      const recNorm = normalizeAddress(p.address || "");
      return recNorm === searchNorm || recNorm.includes(searchNorm) || searchNorm.includes(recNorm);
    });

    if (!matches.length) {
      return { content: [{ type: "text", text: "No building permits found for that address after normalization." }] };
    }
    const formatted = matches.map((p: any) =>
      Object.entries(p)
        .map(([k, v]) => `${k}: ${v}`)
        .join('\n')
    );
    return { content: [{ type: "text", text: formatted.join("\n---\n") }] };
  }
);

// 311 Requests Tool
server.tool(
  "get_311_requests",
  "Fetch Boston 311 requests (all fields, fuzzy address match)",
  {
    address: z.string().optional().describe("Street address or location (partial allowed)"),
    case_status: z.string().optional().describe("Case status (e.g. 'Open', 'Closed')"),
    type: z.string().optional().describe("Request type/category (optional)"),
    limit: z.number().min(1).max(100).default(10).describe("Number of results"),
  },
  async ({ address, case_status, type, limit }) => {
    const params: Record<string, string> = {
      resource_id: SERVICE_RESOURCE_ID,
      limit: limit.toString(),
    };

    if (address) params.q = address;
    if (type) params.q = params.q ? `${params.q} ${type}` : type;
    if (case_status) params.filters = JSON.stringify({ case_status });

    const url = `${CKAN_HOST}/api/3/action/datastore_search?${new URLSearchParams(params).toString()}`;
    const response = await fetch(url, { headers: { 'Content-Type': 'application/json' } });
    if (!response.ok) {
      return { content: [{ type: "text", text: `API Error: ${response.status}` }] };
    }
    const data = await response.json();
    if (!data.success) {
      return { content: [{ type: "text", text: "API call unsuccessful." }] };
    }
    let requests = data.result.records;

    // If address is provided, further filter using normalizeAddress
    if (address) {
      const normAddr = normalizeAddress(address);
      requests = requests.filter((r: any) => {
        // Use location_street_name field, fallback to location
        const recAddr = normalizeAddress(r.location_street_name || r.location || "");
        return recAddr === normAddr;
      });
    }

    if (!requests.length) {
      return { content: [{ type: "text", text: "No 311 requests found for those filters." }] };
    }

    const formatted = requests.map((r: any) =>
      Object.entries(r)
        .map(([k, v]) => `${k}: ${v}`)
        .join('\n')
    );
    return { content: [{ type: "text", text: formatted.join("\n---\n") }] };
  }
);

async function fetchAllCkanRecords(resource_id: string, filters: any, q: string | undefined) {
  let allRecords: any[] = [];
  let offset = 0;
  const limit = 100; // CKAN max per request
  let total = Infinity;

  while (offset < total) {
    const params: Record<string, string> = {
      resource_id,
      limit: limit.toString(),
      offset: offset.toString(),
    };
    if (filters) params.filters = JSON.stringify(filters);
    if (q) params.q = q;
    const url = `${CKAN_HOST}/api/3/action/datastore_search?${new URLSearchParams(params).toString()}`;
    const response = await fetch(url, { headers: { 'Content-Type': 'application/json' } });
    if (!response.ok) throw new Error(`API Error: ${response.status}`);
    const data = await response.json();
    if (!data.success) throw new Error("API call unsuccessful.");
    const records = data.result.records;
    total = data.result.total || (offset + records.length); // If API doesn't provide, just keep going
    allRecords = allRecords.concat(records);
    offset += records.length;
    if (records.length < limit) break; // Last page
  }
  return allRecords;
}

// Food Inspections
async function fetchFoodChunk(q: string | undefined, filters: any, offset: number) {
  const params: Record<string, string> = {
    resource_id: FOOD_RESOURCE_ID,
    limit: PAGE_SIZE.toString(),
    offset: offset.toString(),
  };
  if (q) params.q = q;
  if (filters) params.filters = JSON.stringify(filters);

  const url = `${CKAN_HOST}/api/3/action/datastore_search?${new URLSearchParams(params).toString()}`;
  const response = await fetch(url, { headers: { 'Content-Type': 'application/json' } });
  if (!response.ok) throw new Error(`API Error: ${response.status}`);
  const data = await response.json();
  if (!data.success) throw new Error("API call unsuccessful.");
  return data.result.records;
}

async function fetchAllFoodViolations(q: string | undefined, filters: any): Promise<any[]> {
  // Get total count
  const params: Record<string, string> = {
    resource_id: FOOD_RESOURCE_ID,
    limit: '1',
    offset: '0'
  };
  if (q) params.q = q;
  if (filters) params.filters = JSON.stringify(filters);

  const url = `${CKAN_HOST}/api/3/action/datastore_search?${new URLSearchParams(params).toString()}`;
  const initialRes = await fetch(url, { headers: { 'Content-Type': 'application/json' } });
  if (!initialRes.ok) throw new Error(`API Error: ${initialRes.status}`);
  const initialData = await initialRes.json();
  const total = initialData.result.total || 0;
  if (total === 0) return [];

  // Generate offsets
  const offsets: number[] = [];
  for (let i = 0; i < total; i += PAGE_SIZE) offsets.push(i);

  // Fetch all in parallel batches
  let allRecords: any[] = [];
  for (let i = 0; i < offsets.length; i += PARALLEL_REQUESTS) {
    const chunkOffsets = offsets.slice(i, i + PARALLEL_REQUESTS);
    const chunkResults = await Promise.all(
      chunkOffsets.map(offset => fetchFoodChunk(q, filters, offset))
    );
    chunkResults.forEach(records => allRecords.push(...records));
  }
  return allRecords;
}

// MCP tool
server.tool(
  "get_food_service_violations",
  "Fetch all Boston food service violations (fuzzy search, all results, all fields)",
  {
    businessname: z.string().optional().describe("Restaurant name (fuzzy, partial allowed)"),
    address: z.string().optional().describe("Street address (fuzzy, partial allowed)"),
    zip: z.string().optional().describe("ZIP code (exact match)"),
    comments: z.string().optional().describe("Comments (fuzzy, partial allowed)"),
  },
  async ({ businessname, address, zip, comments }) => {
    // Build q string for fuzzy search across requested fields
    let qParts: string[] = [];
    if (businessname) qParts.push(businessname);
    if (address) qParts.push(address);
    if (comments) qParts.push(comments);
    const q = qParts.length ? qParts.join(" ") : undefined;

    // Use filters for zip (CKAN filters are exact!)
    const filters = zip ? { zip } : undefined;

    // Fetch ALL matching records (may be thousands)
    let records: any[];
    try {
      records = await fetchAllFoodViolations(q, filters);
    } catch (e) {
      return { content: [{ type: "text", text: `Failed to fetch records: ${e}` }] };
    }

    if (!records.length) {
      return { content: [{ type: "text", text: "No food service violations found for those filters." }] };
    }

    // Format all fields for each record
    const formatted = records.map((r: any) =>
      Object.entries(r)
        .map(([k, v]) => `${k}: ${v}`)
        .join('\n')
    );
    return { content: [{ type: "text", text: formatted.join("\n---\n") }] };
  }
);


// Helper: fetch all crime records in parallel (adapted from earlier patterns)
async function fetchAllCrimeRecords(q: string | undefined, filters: any): Promise<any[]> {
  const params: Record<string, string> = {
    resource_id: CRIME_RESOURCE_ID,
    limit: '1',
    offset: '0'
  };
  if (q) params.q = q;
  if (filters && Object.keys(filters).length) params.filters = JSON.stringify(filters);

  const url = `${CKAN_HOST}/api/3/action/datastore_search?${new URLSearchParams(params).toString()}`;
  const initialRes = await fetch(url, { headers: { 'Content-Type': 'application/json' } });
  if (!initialRes.ok) throw new Error(`API Error: ${initialRes.status}`);
  const initialData = await initialRes.json();
  const total = initialData.result.total || 0;
  if (total === 0) return [];

  // Parallel chunking
  const offsets: number[] = [];
  for (let i = 0; i < total; i += PAGE_SIZE) offsets.push(i);

  let allRecords: any[] = [];
  for (let i = 0; i < offsets.length; i += PARALLEL_REQUESTS) {
    const chunkOffsets = offsets.slice(i, i + PARALLEL_REQUESTS);
    const chunkResults = await Promise.all(
      chunkOffsets.map(offset => {
        const chunkParams: Record<string, string> = {
          resource_id: CRIME_RESOURCE_ID,
          limit: PAGE_SIZE.toString(),
          offset: offset.toString()
        };
        if (q) chunkParams.q = q;
        if (filters && Object.keys(filters).length) chunkParams.filters = JSON.stringify(filters);
        const chunkUrl = `${CKAN_HOST}/api/3/action/datastore_search?${new URLSearchParams(chunkParams).toString()}`;
        return fetch(chunkUrl, { headers: { 'Content-Type': 'application/json' } })
          .then(res => res.json())
          .then(data => data.result.records);
      })
    );
    chunkResults.forEach(records => allRecords.push(...records));
  }
  return allRecords;
}

// Summarize top N for a field
function countAndSummarize(records: any[], field: string, top: number) {
  const counts: Record<string, number> = {};
  for (const r of records) {
    const val = (r[field] || "UNKNOWN").trim();
    counts[val] = (counts[val] || 0) + 1;
  }
  return Object.entries(counts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, top)
    .map(([val, count], idx) => `${idx + 1}. ${val}: ${count}`)
    .join("\n");
}

// MCP tool
server.tool(
  "get_crime_reports",
  {
    street: z.string().optional(),
    offense_description: z.string().optional(),
    district: z.string().optional(),
    year: z.string().optional(),
    month: z.string().optional(),
    day_of_week: z.string().optional(),
    hour: z.string().optional(),
    shooting: z.string().optional(),
  },
  {
    title: "Get Crime Reports",
    description: "Fetch Boston crime reports (fuzzy street and offense description, supports additional filters)"
  },
  async ({
    street,
    offense_description,
    district,
    year,
    month,
    day_of_week,
    hour,
    shooting,
  }) => {
    // Build q string from uppercased fuzzy fields
    let qParts: string[] = [];
    if (street) qParts.push(street.trim().toUpperCase());
    if (offense_description) qParts.push(offense_description.trim().toUpperCase());
    const q = qParts.length ? qParts.join(" ") : undefined;

    // Build filters with UPPERCASE field names and values
    let filters: any = {};
    if (district) filters.DISTRICT = district.toUpperCase();
    if (year) filters.YEAR = year;
    if (month) filters.MONTH = month;
    // if (day_of_week) filters.DAY_OF_WEEK = day_of_week.toUpperCase();
    if (hour) filters.HOUR = hour;
    if (shooting) filters.SHOOTING = shooting;

    let records: any[];
    try {
      records = await fetchAllCrimeRecords(q, filters);
    } catch (e) {
      return { content: [{ type: "text", text: `Failed to fetch records: ${e}` }] };
    }

    if (!records.length) {
      return { content: [{ type: "text", text: "No crime reports found for those filters." }] };
    }

    // Further filter for fuzzy street/offense match, if provided
    if (street) {
      const streetNorm = street.trim().toUpperCase();
      records = records.filter(
        (r: any) => (r.STREET || "").toUpperCase().includes(streetNorm)
      );
    }
    if (offense_description) {
      const descNorm = offense_description.trim().toUpperCase();
      records = records.filter(
        (r: any) => (r.OFFENSE_DESCRIPTION || "").toUpperCase().includes(descNorm)
      );
    }
     if (day_of_week) {
       const dayNorm = day_of_week.trim().toUpperCase();
  records = records.filter(
    (r: any) => (r.DAY_OF_WEEK || "").trim().toUpperCase() === dayNorm
  );
    }

    // Summarize top 10 offense types
    const topOffenses = countAndSummarize(records, "OFFENSE_DESCRIPTION", 10);

    // Return the summary for now (CSV attachment commented out)
    return {
      content: [
        { 
          type: "text", 
          text: `Found ${records.length} crime reports.\n\nTop 10 offense types in your query:\n${topOffenses}` 
        }
      ]
    };

    // Attach CSV file (uncomment when ready to use)
    // const csv = parse(records);
    // return {
    //   content: [
    //     { type: "text", text: `Top 10 offense types in your query:\n${topOffenses}\n\nSee attached CSV for full results.` },
    //     {
    //       type: "file",
    //       filename: "crime_reports.csv",
    //       mimeType: "text/csv",
    //       data: Buffer.from(csv).toString("base64")
    //     }
    //   ]
    // };
  }
);


// Helper: Fetch a single chunk
async function fetchCheckbookChunk(q: string | undefined, filters: any, offset: number) {
  const params: Record<string, string> = {
    resource_id: CHECKBOOK_RESOURCE_ID,
    limit: PAGE_SIZE.toString(),
    offset: offset.toString(),
  };
  if (q) params.q = q;
  if (filters) params.filters = JSON.stringify(filters);
  const url = `${CKAN_HOST}/api/3/action/datastore_search?${new URLSearchParams(params).toString()}`;
  const response = await fetch(url, { headers: { 'Content-Type': 'application/json' } });
  if (!response.ok) throw new Error(`API Error: ${response.status}`);
  const data = await response.json();
  if (!data.success) throw new Error("API call unsuccessful.");
  return data.result.records;
}

// Fetch all records with chunking
async function fetchAllCheckbookRecords(q: string | undefined, filters: any): Promise<any[]> {
  const params: Record<string, string> = {
    resource_id: CHECKBOOK_RESOURCE_ID,
    limit: '1',
    offset: '0'
  };
  if (q) params.q = q;
  if (filters) params.filters = JSON.stringify(filters);
  const url = `${CKAN_HOST}/api/3/action/datastore_search?${new URLSearchParams(params).toString()}`;
  const initialRes = await fetch(url, { headers: { 'Content-Type': 'application/json' } });
  if (!initialRes.ok) throw new Error(`API Error: ${initialRes.status}`);
  const initialData = await initialRes.json();
  const total = initialData.result.total || 0;
  if (total === 0) return [];
  const offsets: number[] = [];
  for (let i = 0; i < total; i += PAGE_SIZE) offsets.push(i);

  let allRecords: any[] = [];
  for (let i = 0; i < offsets.length; i += PARALLEL_REQUESTS) {
    const chunkOffsets = offsets.slice(i, i + PARALLEL_REQUESTS);
    const chunkResults = await Promise.all(
      chunkOffsets.map(offset => fetchCheckbookChunk(q, filters, offset))
    );
    chunkResults.forEach(records => allRecords.push(...records));
  }
  return allRecords;
}

// MCP tool
server.tool(
  "get_checkbook_records",
  {
    vendor: z.string().optional().describe("Vendor name (fuzzy, partial allowed)"),
    dept: z.string().optional().describe("Department name (fuzzy, partial allowed)"),
    fiscal_year: z.string().optional().describe("Fiscal year (optional)"),
    year: z.string().optional().describe("Calendar year (optional)"),
    month: z.string().optional().describe("Month (optional, e.g. 'July' or '7')"),
    top: z.number().min(1).max(2000).default(10).describe("How many top vendors to display (by total spend)"),
  },
  {
    title: "Get Checkbook Explorer Records",
    description: "Search Boston checkbook transactions by vendor, department, year, or month."
  },
  async ({ vendor, dept, fiscal_year, year, month, top = 2000 }) => {
    // Build q for fuzzy search (for vendor/dept)
    let qParts: string[] = [];
    if (vendor) qParts.push(vendor);
    if (dept) qParts.push(dept);
    const q = qParts.length ? qParts.join(" ") : undefined;

    // Filters for exact fields (fiscal_year, year, month, etc)
    let filters: any = {};
    if (fiscal_year) filters["Fiscal Year"] = fiscal_year;
    if (year) filters["Year"] = year;
    if (month) filters["Month"] = month;

    let records: any[];
    try {
      records = await fetchAllCheckbookRecords(q, filters);
    } catch (e) {
      return { content: [{ type: "text", text: `Failed to fetch checkbook records: ${e}` }] };
    }
    if (!records.length) {
      return { content: [{ type: "text", text: "No checkbook records found for those filters." }] };
    }

    // Summarize: e.g., top 10 vendors by total spend
    const totals: Record<string, number> = {};
    for (const r of records) {
      const v = (r["Vendor Name"] || "UNKNOWN").trim();
      const amt = parseFloat((r["Monetary Amount"] || "0").replace(/[^0-9.-]+/g,""));
      totals[v] = (totals[v] || 0) + (isNaN(amt) ? 0 : amt);
    }
    const topVendors = Object.entries(totals)
      .sort((a, b) => b[1] - a[1])
      .slice(0, top)
      .map(([name, amt], idx) => `${idx+1}. ${name}: $${amt.toLocaleString()}`);

    // CSV download (optional)
    // import { parse } from "json2csv";
    // const csv = parse(records);

    return {
      content: [
        { type: "text", text: `Found ${records.length} records.\n\nTop 10 vendors by total spend:\n${topVendors.join("\n")}` },
        // Uncomment to attach CSV:
        // {
        //   type: "file",
        //   filename: "checkbook_records.csv",
        //   mimeType: "text/csv",
        //   data: Buffer.from(csv).toString("base64")
        // }
      ]
    };
  }
);



// Run server (stdio)
async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("Boston MCP Server running...");
}
main().catch(err => {
  console.error("Fatal error:", err);
  process.exit(1);
});
