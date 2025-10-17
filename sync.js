// CoinGecko to Webflow CMS Sync Script
const fetch = require('node-fetch');

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
    
    // Step 2: Get existing items from Webflow
    const existingItems = await getAllWebflowItems();
    console.log(`Found ${existingItems.length} existing items in Webflow`);
    
    // Step 3: Transform and sync data
    const result = await syncItems(coins, existingItems);
    console.log('Sync completed successfully');
    
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

// Get all existing items from Webflow
async function getAllWebflowItems() {
  const items = [];
  let offset = 0;
  const limit = 100;
  
  while (true) {
    const response = await fetch(
      `https://api.webflow.com/v2/collections/${WEBFLOW_COLLECTION_ID}/items?limit=${limit}&offset=${offset}`,
      {
        headers: {
          'Authorization': `Bearer ${WEBFLOW_API_TOKEN}`,
          'accept': 'application/json'
        }
      }
    );
    
    if (!response.ok) {
      throw new Error(`Failed to fetch Webflow items: ${response.status}`);
    }
    
    const data = await response.json();
    items.push(...data.items);
    
    if (data.items.length < limit) {
      break;
    }
    
    offset += limit;
  }
  
  return items;
}

// Sync items - update existing or create new
async function syncItems(coins, existingItems) {
  const results = { updated: 0, created: 0, failed: 0 };
  
  // Create a map of existing items by coingecko-id
  const existingMap = new Map();
  existingItems.forEach(item => {
    const coingeckoId = item.fieldData?.['coingecko-id'];
    if (coingeckoId) {
      existingMap.set(coingeckoId, item);
    }
  });
  
  console.log(`\nProcessing ${coins.length} coins...`);
  
  for (let i = 0; i < coins.length; i++) {
    const coin = coins[i];
    const coingeckoId = coin.id;
    
    try {
      const fieldData = {
        'name': coin.name || '',
        'symbol': coin.symbol?.toUpperCase() || '',
        'logo-url': coin.image || '',
        'price': coin.current_price?.toString() || '0',
        'change-24h': coin.price_change_percentage_24h?.toString() || '0',
        'change-7d': coin.price_change_percentage_7d_in_currency?.toString() || '0',
        'change-30d': coin.price_change_percentage_30d_in_currency?.toString() || '0',
        'market-cap': coin.market_cap?.toString() || '0',
        'volume-24h': coin.total_volume?.toString() || '0',
        'circulating-supply': coin.circulating_supply?.toString() || '0',
        'total-supply': coin.total_supply?.toString() || '0',
        'coingecko-id': coingeckoId,
        'last-updated': new Date().toISOString()
      };
      
      const existingItem = existingMap.get(coingeckoId);
      
      if (existingItem) {
        // Update existing item
        console.log(`[${i + 1}/${coins.length}] Updating: ${coin.name}`);
        
        const response = await fetch(
          `https://api.webflow.com/v2/collections/${WEBFLOW_COLLECTION_ID}/items/${existingItem.id}`,
          {
            method: 'PATCH',
            headers: {
              'Authorization': `Bearer ${WEBFLOW_API_TOKEN}`,
              'Content-Type': 'application/json',
              'accept': 'application/json'
            },
            body: JSON.stringify({ fieldData })
          }
        );
        
        if (response.ok) {
          results.updated++;
          console.log(`  ✅ Updated`);
        } else {
          const errorText = await response.text();
          console.error(`  ❌ Failed (${response.status}): ${errorText}`);
          results.failed++;
        }
      } else {
        // Create new item
        console.log(`[${i + 1}/${coins.length}] Creating: ${coin.name}`);
        
        const response = await fetch(
          `https://api.webflow.com/v2/collections/${WEBFLOW_COLLECTION_ID}/items`,
          {
            method: 'POST',
            headers: {
              'Authorization': `Bearer ${WEBFLOW_API_TOKEN}`,
              'Content-Type': 'application/json',
              'accept': 'application/json'
            },
            body: JSON.stringify({
              fieldData,
              isArchived: false,
              isDraft: false
            })
          }
        );
        
        if (response.ok) {
          results.created++;
          console.log(`  ✅ Created`);
        } else {
          const errorText = await response.text();
          console.error(`  ❌ Failed (${response.status}): ${errorText}`);
          results.failed++;
        }
      }
      
      // Small delay to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 100));
      
    } catch (error) {
      console.error(`[${i + 1}/${coins.length}] Error processing ${coin.name}:`, error.message);
      results.failed++;
    }
  }
  
  return results;
}

// Log results
function logResults(results) {
  console.log('\n' + '='.repeat(50));
  console.log('SYNC RESULTS');
  console.log('='.repeat(50));
  console.log(`✅ Updated: ${results.updated}`);
  console.log(`✨ Created: ${results.created}`);
  console.log(`❌ Failed: ${results.failed}`);
  console.log(`📊 Total: ${results.updated + results.created + results.failed}`);
  console.log('='.repeat(50));
}

// Run the sync
syncCoinsToWebflow()
  .then(() => {
    console.log('\n🎉 Sync completed successfully');
    process.exit(0);
  })
  .catch(error => {
    console.error('\n💥 Sync failed:', error);
    process.exit(1);
  });
