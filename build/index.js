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
// Building Permits Tool
// server.tool(
//   "get_building_permits",
//   "Fetch recent building permits in Boston",
//   {
//     limit: z.number().min(1).max(50).default(5).describe("Number of results"),
//   },
//   async ({ limit }) => {
//     const url = `${PERMIT_API}?$top=${limit}&$orderby=issued_date desc`;
//     const data = await fetchJSON<any>(url);
//     if (!data?.value) return { content: [{ type: "text", text: "No permit data found." }] };
//     const results = data.value.map((item: any) =>
//       `Address: ${item.address}\nType: ${item.permit_type}\nIssued: ${item.issued_date}`
//     );
//     return { content: [{ type: "text", text: results.join("\n---\n") }] };
//   }
// );
function normalizeAddress(address) {
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
async function fetchPermitChunk(address, offset) {
    const params = new URLSearchParams({
        resource_id: BUILDING_RESOURCE_ID,
        q: address,
        limit: PAGE_SIZE.toString(),
        offset: offset.toString(),
    });
    const url = `${CKAN_HOST}/api/3/action/datastore_search?${params.toString()}`;
    const response = await fetch(url, { headers: { 'Content-Type': 'application/json' } });
    if (!response.ok)
        throw new Error(`API Error: ${response.status}`);
    const data = await response.json();
    if (!data.success)
        throw new Error("API call unsuccessful.");
    return data.result.records;
}
// Fetch all candidate records in parallel
async function fetchAllPermitsParallel(address) {
    // First, get total count for this address via q (broad, not normalized)
    const params = new URLSearchParams({
        resource_id: BUILDING_RESOURCE_ID,
        q: address,
        limit: '1',
        offset: '0'
    });
    const url = `${CKAN_HOST}/api/3/action/datastore_search?${params.toString()}`;
    const initialRes = await fetch(url, { headers: { 'Content-Type': 'application/json' } });
    if (!initialRes.ok)
        throw new Error(`API Error: ${initialRes.status}`);
    const initialData = await initialRes.json();
    const total = initialData.result.total || 0;
    if (total === 0)
        return [];
    // Calculate all needed offsets
    const offsets = [];
    for (let i = 0; i < total; i += PAGE_SIZE)
        offsets.push(i);
    // Fetch all in parallel batches
    let allRecords = [];
    for (let i = 0; i < offsets.length; i += PARALLEL_REQUESTS) {
        const chunkOffsets = offsets.slice(i, i + PARALLEL_REQUESTS);
        const chunkResults = await Promise.all(chunkOffsets.map(offset => fetchPermitChunk(address, offset)));
        chunkResults.forEach(records => allRecords.push(...records));
    }
    return allRecords;
}
server.tool("get_building_permits_fuzzy_parallel", "Fetch ALL building permits for an address (parallel, fuzzy match)", {
    address: z.string().describe("Street address to search for (e.g., 65 Commonwealth Ave)"),
}, async ({ address }) => {
    const normalizedAddress = normalizeAddress(address);
    let allRecords;
    try {
        allRecords = await fetchAllPermitsParallel(normalizedAddress);
    }
    catch (e) {
        return { content: [{ type: "text", text: `Failed to fetch permits: ${e}` }] };
    }
    if (!allRecords.length) {
        return { content: [{ type: "text", text: "No building permits found for that address." }] };
    }
    // Normalize & filter (strict + forgiving match)
    const searchNorm = normalizeAddress(address);
    const matches = allRecords.filter((p) => {
        const recNorm = normalizeAddress(p.address || "");
        return recNorm === searchNorm || recNorm.includes(searchNorm) || searchNorm.includes(recNorm);
    });
    if (!matches.length) {
        return { content: [{ type: "text", text: "No building permits found for that address after normalization." }] };
    }
    const formatted = matches.map((p) => Object.entries(p)
        .map(([k, v]) => `${k}: ${v}`)
        .join('\n'));
    return { content: [{ type: "text", text: formatted.join("\n---\n") }] };
});
// 311 Requests Tool
server.tool("get_311_requests", "Fetch Boston 311 requests (all fields, fuzzy address match)", {
    address: z.string().optional().describe("Street address or location (partial allowed)"),
    case_status: z.string().optional().describe("Case status (e.g. 'Open', 'Closed')"),
    type: z.string().optional().describe("Request type/category (optional)"),
    limit: z.number().min(1).max(100).default(10).describe("Number of results"),
}, async ({ address, case_status, type, limit }) => {
    const params = {
        resource_id: SERVICE_RESOURCE_ID,
        limit: limit.toString(),
    };
    if (address)
        params.q = address;
    if (type)
        params.q = params.q ? `${params.q} ${type}` : type;
    if (case_status)
        params.filters = JSON.stringify({ case_status });
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
        requests = requests.filter((r) => {
            // Use location_street_name field, fallback to location
            const recAddr = normalizeAddress(r.location_street_name || r.location || "");
            return recAddr === normAddr;
        });
    }
    if (!requests.length) {
        return { content: [{ type: "text", text: "No 311 requests found for those filters." }] };
    }
    const formatted = requests.map((r) => Object.entries(r)
        .map(([k, v]) => `${k}: ${v}`)
        .join('\n'));
    return { content: [{ type: "text", text: formatted.join("\n---\n") }] };
});
async function fetchAllCkanRecords(resource_id, filters, q) {
    let allRecords = [];
    let offset = 0;
    const limit = 100; // CKAN max per request
    let total = Infinity;
    while (offset < total) {
        const params = {
            resource_id,
            limit: limit.toString(),
            offset: offset.toString(),
        };
        if (filters)
            params.filters = JSON.stringify(filters);
        if (q)
            params.q = q;
        const url = `${CKAN_HOST}/api/3/action/datastore_search?${new URLSearchParams(params).toString()}`;
        const response = await fetch(url, { headers: { 'Content-Type': 'application/json' } });
        if (!response.ok)
            throw new Error(`API Error: ${response.status}`);
        const data = await response.json();
        if (!data.success)
            throw new Error("API call unsuccessful.");
        const records = data.result.records;
        total = data.result.total || (offset + records.length); // If API doesn't provide, just keep going
        allRecords = allRecords.concat(records);
        offset += records.length;
        if (records.length < limit)
            break; // Last page
    }
    return allRecords;
}
// Food Inspections
async function fetchFoodChunk(q, filters, offset) {
    const params = {
        resource_id: FOOD_RESOURCE_ID,
        limit: PAGE_SIZE.toString(),
        offset: offset.toString(),
    };
    if (q)
        params.q = q;
    if (filters)
        params.filters = JSON.stringify(filters);
    const url = `${CKAN_HOST}/api/3/action/datastore_search?${new URLSearchParams(params).toString()}`;
    const response = await fetch(url, { headers: { 'Content-Type': 'application/json' } });
    if (!response.ok)
        throw new Error(`API Error: ${response.status}`);
    const data = await response.json();
    if (!data.success)
        throw new Error("API call unsuccessful.");
    return data.result.records;
}
async function fetchAllFoodViolations(q, filters) {
    // Get total count
    const params = {
        resource_id: FOOD_RESOURCE_ID,
        limit: '1',
        offset: '0'
    };
    if (q)
        params.q = q;
    if (filters)
        params.filters = JSON.stringify(filters);
    const url = `${CKAN_HOST}/api/3/action/datastore_search?${new URLSearchParams(params).toString()}`;
    const initialRes = await fetch(url, { headers: { 'Content-Type': 'application/json' } });
    if (!initialRes.ok)
        throw new Error(`API Error: ${initialRes.status}`);
    const initialData = await initialRes.json();
    const total = initialData.result.total || 0;
    if (total === 0)
        return [];
    // Generate offsets
    const offsets = [];
    for (let i = 0; i < total; i += PAGE_SIZE)
        offsets.push(i);
    // Fetch all in parallel batches
    let allRecords = [];
    for (let i = 0; i < offsets.length; i += PARALLEL_REQUESTS) {
        const chunkOffsets = offsets.slice(i, i + PARALLEL_REQUESTS);
        const chunkResults = await Promise.all(chunkOffsets.map(offset => fetchFoodChunk(q, filters, offset)));
        chunkResults.forEach(records => allRecords.push(...records));
    }
    return allRecords;
}
// MCP tool
server.tool("get_food_service_violations", "Fetch all Boston food service violations (fuzzy search, all results, all fields)", {
    businessname: z.string().optional().describe("Restaurant name (fuzzy, partial allowed)"),
    address: z.string().optional().describe("Street address (fuzzy, partial allowed)"),
    city: z.string().optional().describe("City (optional, partial allowed)"),
    state: z.string().optional().describe("State (optional, partial allowed)"),
    zip: z.string().optional().describe("ZIP code (exact match)"),
    comments: z.string().optional().describe("Comments (fuzzy, partial allowed)"),
}, async ({ businessname, address, city, state, zip, comments }) => {
    // Build q string for fuzzy search across requested fields
    let qParts = [];
    if (businessname)
        qParts.push(businessname);
    if (address)
        qParts.push(address);
    if (city)
        qParts.push(city);
    if (state)
        qParts.push(state);
    if (comments)
        qParts.push(comments);
    const q = qParts.length ? qParts.join(" ") : undefined;
    // Use filters for zip (CKAN filters are exact!)
    const filters = zip ? { zip } : undefined;
    // Fetch ALL matching records (may be thousands)
    let records;
    try {
        records = await fetchAllFoodViolations(q, filters);
    }
    catch (e) {
        return { content: [{ type: "text", text: `Failed to fetch records: ${e}` }] };
    }
    if (!records.length) {
        return { content: [{ type: "text", text: "No food service violations found for those filters." }] };
    }
    // Format all fields for each record
    const formatted = records.map((r) => Object.entries(r)
        .map(([k, v]) => `${k}: ${v}`)
        .join('\n'));
    return { content: [{ type: "text", text: formatted.join("\n---\n") }] };
});
// server.tool(
//   "get_food_service_violations",
//   "Fetch Boston food service violations (fuzzy business name, optional filters)",
//   {
//     businessname: z.string().optional().describe("Business name to search (fuzzy, partial allowed)"),
//     address: z.string().optional().describe("Street address (fuzzy, partial allowed)"),
//     city: z.string().optional().describe("City (optional, partial allowed)"),
//     state: z.string().optional().describe("State (optional, partial allowed)"),
//     zip: z.string().optional().describe("ZIP code"),
//     comments: z.string().optional().describe("Comments (fuzzy, partial allowed)"),
//     limit: z.number().min(1).max(100).default(10).describe("Number of results"),
//   },
//   async ({ businessname, address, city, state, zip, comments, limit }) => {
//     const params: Record<string, string> = {
//       resource_id: FOOD_RESOURCE_ID,
//       limit: limit.toString(),
//     };
//     // Build q string for fuzzy search across requested fields
//     let qParts: string[] = [];
//     if (businessname) qParts.push(businessname);
//     if (address) qParts.push(address);
//     if (city) qParts.push(city);
//     if (state) qParts.push(state);
//     if (comments) qParts.push(comments);
//     if (qParts.length) params.q = qParts.join(" ");
//     // Use filters for zip (CKAN filters are exact!)
//     if (zip) params.filters = JSON.stringify({ zip });
//     const url = `${CKAN_HOST}/api/3/action/datastore_search?${new URLSearchParams(params).toString()}`;
//     const response = await fetch(url, { headers: { 'Content-Type': 'application/json' } });
//     if (!response.ok) {
//       return { content: [{ type: "text", text: `API Error: ${response.status}` }] };
//     }
//     const data = await response.json();
//     if (!data.success) {
//       return { content: [{ type: "text", text: "API call unsuccessful." }] };
//     }
//     let requests = data.result.records;
//     // If address is supplied, filter results by normalized address (for best precision)
//     if (address) {
//       const normAddr = normalizeAddress(address);
//       requests = requests.filter((r: any) => {
//         // Try matching to both address and businessaddress, if present
//         const recAddr = normalizeAddress(r.address || r.businessaddress || "");
//         return recAddr === normAddr;
//       });
//     }
//     if (!requests.length) {
//       return { content: [{ type: "text", text: "No food service violations found for those filters." }] };
//     }
//     const formatted = requests.map((r: any) =>
//       Object.entries(r)
//         .map(([k, v]) => `${k}: ${v}`)
//         .join('\n')
//     );
//     return { content: [{ type: "text", text: formatted.join("\n---\n") }] };
//   }
// );
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
