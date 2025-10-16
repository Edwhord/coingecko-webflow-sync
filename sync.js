// CoinGecko to Webflow CMS Sync Script
// This script fetches top 100 coins and updates Webflow CMS

const fetch = require('node-fetch');

// Get config from environment variables (set in Render)
const WEBFLOW_API_TOKEN = process.env.WEBFLOW_API_TOKEN;
const WEBFLOW_COLLECTION_ID = process.env.WEBFLOW_COLLECTION_ID;
const COINGECKO_API_URL = 'https://api.coingecko.com/api/v3/coins/markets';

// Main function
async function syncCoinsToWebflow() {
  try {
    console.log('Starting sync...');
    
    // Step 1: Fetch top 100 coins from CoinGecko
    const coins = await fetchTopCoins();
    console.log(`Fetched ${coins.length} coins from CoinGecko`);
    
    // Step 2: Transform data to Webflow format
    const webflowItems = transformToWebflowFormat(coins);
    console.log('Data transformed to Webflow format');
    
    // Step 3: Send to Webflow CMS (bulk update)
    const result = await updateWebflowCMS(webflowItems);
    console.log('Webflow CMS updated successfully');
    
    // Step 4: Log results
    logResults(result);
    
    return result;
  } catch (error) {
    console.error('Error during sync:', error.message);
    throw error;
  }
}

// Fetch top 100 coins from CoinGecko
async function fetchTopCoins() {
  const params = new URLSearchParams({
    vs_currency: 'usd',
    order: 'market_cap_desc',
    per_page: 100,
    page: 1,
    sparkline: false,
    price_change_percentage: '24h,7d,30d,1y'
  });
  
  const response = await fetch(`${COINGECKO_API_URL}?${params}`);
  
  if (!response.ok) {
    throw new Error(`CoinGecko API error: ${response.status}`);
  }
  
  return await response.json();
}

// Transform CoinGecko data to Webflow format
function transformToWebflowFormat(coins) {
  return coins.map(coin => ({
    fieldData: {
      'name': coin.name || '',
      'symbol': coin.symbol?.toUpperCase() || '',
      'logo-url': coin.image || '',
      'price': coin.current_price?.toString() || '0',
      'change-24h': coin.price_change_percentage_24h?.toString() || '0',
      'change-7d': coin.price_change_percentage_7d_in_currency?.toString() || '0',
      'change-30d': coin.price_change_percentage_30d_in_currency?.toString() || '0',
      'change-1y': coin.price_change_percentage_1y_in_currency?.toString() || '0',
      'market-cap': coin.market_cap?.toString() || '0',
      'volume-24h': coin.total_volume?.toString() || '0',
      'circulating-supply': coin.circulating_supply?.toString() || '0',
      'total-supply': coin.total_supply?.toString() || '0',
      'ath-usd': coin.ath?.toString() || '0',
      'atl-usd': coin.atl?.toString() || '0',
      'coingecko-id': coin.id || '',
      'last-updated': new Date().toISOString()
    }
  }));
}

// Update Webflow CMS using bulk API
async function updateWebflowCMS(items) {
  // Webflow bulk API has a limit, so we may need to batch
  const batchSize = 100;
  const batches = [];
  
  for (let i = 0; i < items.length; i += batchSize) {
    batches.push(items.slice(i, i + batchSize));
  }
  
  const results = [];
  
  for (const batch of batches) {
    const response = await fetch(
      `https://api.webflow.com/v2/collections/${WEBFLOW_COLLECTION_ID}/items/bulk`,
      {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${WEBFLOW_API_TOKEN}`,
          'Content-Type': 'application/json',
          'accept': 'application/json'
        },
        body: JSON.stringify({ items: batch })
      }
    );
    
    if (!response.ok) {
      const error = await response.text();
      throw new Error(`Webflow API error: ${response.status} - ${error}`);
    }
    
    const result = await response.json();
    results.push(result);
  }
  
  return results;
}

// Log results
function logResults(results) {
  const timestamp = new Date().toISOString();
  console.log('='.repeat(50));
  console.log(`Sync completed at: ${timestamp}`);
  console.log(`Total batches processed: ${results.length}`);
  results.forEach((result, index) => {
    console.log(`Batch ${index + 1}:`, JSON.stringify(result, null, 2));
  });
  console.log('='.repeat(50));
}

// Run the sync
syncCoinsToWebflow()
  .then(() => {
    console.log('Sync completed successfully');
    process.exit(0);
  })
  .catch(error => {
    console.error('Sync failed:', error);
    process.exit(1);
  });
