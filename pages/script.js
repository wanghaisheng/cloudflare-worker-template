document.getElementById('actionForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const action = document.getElementById('action').value;
    const version = document.getElementById('version').value;

    const response = await fetch(`https://your-worker-subdomain.workers.dev/?action=${action}&version=${version}`, {
        method: 'GET'
    });

    const result = await response.text();
    document.getElementById('result').innerText = result;
});
