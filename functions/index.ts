import { Client } from 'pg';

export interface Env {
    DB_CONNECTION_STRING: string;  // Add environment variable
}

const client = new Client({
    connectionString: '', // Will be set later
});

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

    await client.query(
        'INSERT INTO update_history (update_date, version, description) VALUES ($1, $2, $3)',
        [updateDate, version, 'Monthly import']
    );
    const updateId = (await client.query(
        'SELECT id FROM update_history WHERE version = $1',
        [version]
    )).rows[0].id;

    for (const item of data) {
        await client.query(
            'INSERT INTO tranco_domains (rank, domain, update_id) VALUES ($1, $2, $3)',
            [item.rank, item.domain, updateId]
        );
    }
}

async function generateReports(version: string): Promise<void> {
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
                'INSERT INTO rank_reports (period, report_date, data, version) VALUES ($1, $2, $3, $4)',
                [period, today, reportData, version]
            );
        }
    }
}

export default {
    async fetch(request: Request, env: Env): Promise<Response> {
        const url = new URL(request.url);
        const action = url.searchParams.get('action');
        const version = url.searchParams.get('version') || 'latest';

        // Set the connection string from the environment variable
        client.connectionString = env.DB_CONNECTION_STRING;

        try {
            await client.connect();

            if (action === 'insert') {
                const csvUrl = 'https://tranco-list.eu/download/YXW2G/full'; // Replace with your CSV URL
                const csvText = await downloadCSV(csvUrl);
                await insertData(csvText);
                return new Response('Data inserted successfully', { status: 200 });
            } else if (action === 'generate') {
                await generateReports(version);
                return new Response('Reports generated successfully', { status: 200 });
            } else {
                return new Response('Invalid action', { status: 400 });
            }
        } catch (error) {
            return new Response(`Error: ${error.message}`, { status: 500 });
        } finally {
            await client.end();
        }
    }
};
