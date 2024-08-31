import { Client } from 'pg';

export interface Env {
    DB: string;
}

// Initialize the database connection
const client = new Client({
    connectionString: 'your-turso-connection-string'
});

// Define helper functions
async function downloadCSV(url: string): Promise<string> {
    const response = await fetch(url);
    if (!response.ok) throw new Error('Failed to fetch CSV');
    return response.text();
}

async function insertData(csvText: string): Promise<void> {
    const lines = csvText.split('\n').slice(1); // Skip header
    const rows = lines.map(line => line.split(','));

    const data = rows.map(row => ({
        rank: parseInt(row[0], 10),
        domain: row[1]
    }));

    const updateDate = new Date().toISOString().split('T')[0];
    const version = `v${updateDate.replace(/-/g, '')}`;

    // Insert into UpdateHistory
    await client.query(
        'INSERT INTO update_history (update_date, version, description) VALUES ($1, $2, $3)',
        [updateDate, version, 'Monthly import']
    );
    const updateId = (await client.query(
        'SELECT id FROM update_history WHERE version = $1',
        [version]
    )).rows[0].id;

    // Insert into TrancoDomain
    for (const item of data) {
        await client.query(
            'INSERT INTO tranco_domains (rank, domain, update_id) VALUES ($1, $2, $3)',
            [item.rank, item.domain, updateId]
        );
    }
}

async function generateReports(): Promise<void> {
    const today = new Date().toISOString().split('T')[0];
    const periods = {
        '1 month': [new Date(new Date().setDate(new Date().getDate() - 30)).toISOString().split('T')[0], today],
        '3 months': [new Date(new Date().setDate(new Date().getDate() - 90)).toISOString().split('T')[0], today],
        '6 months': [new Date(new Date().setDate(new Date().getDate() - 180)).toISOString().split('T')[0], today]
    };

    for (const [period, [startDate, endDate]] of Object.entries(periods)) {
        for (const topN of [100, 10000]) {
            const result = await client.query(`
                SELECT domain, current_rank, previous_rank, (previous_rank - current_rank) AS rank_difference, current_version, previous_version
                FROM (
                    SELECT domain, current_rank, current_version, latest_update_date
                    FROM tranco_domains
                    JOIN update_history ON tranco_domains.update_id = update_history.id
                    WHERE rank <= $1 AND update_date BETWEEN $2 AND $3
                ) AS current
                LEFT JOIN (
                    SELECT domain, current_rank AS previous_rank, current_version AS previous_version
                    FROM tranco_domains
                    JOIN update_history ON tranco_domains.update_id = update_history.id
                    WHERE rank <= $1 AND update_date < $2
                ) AS previous ON current.domain = previous.domain
                ORDER BY domain, current_version;
            `, [topN, startDate, endDate]);

            const reportData = JSON.stringify(result.rows);
            await client.query(
                'INSERT INTO rank_reports (period, report_date, data) VALUES ($1, $2, $3)',
                [period, today, reportData]
            );
        }
    }
}

export default {
    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
        try {
            await client.connect();

            const csvUrl = 'https://example.com/cloudflare_data.csv';
            const csvText = await downloadCSV(csvUrl);
            await insertData(csvText);
            await generateReports();

            return new Response('Reports generated successfully', { status: 200 });
        } catch (error) {
            return new Response(`Error: ${error.message}`, { status: 500 });
        } finally {
            // Close the database connection, but don't block returning the response
            ctx.waitUntil(client.end());
        }
    }
};
