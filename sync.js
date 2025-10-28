// CoinGecko to Webflow + Google Sheets + Chart Data Sync Script
const fetch = require('node-fetch');
const { google } = require('googleapis');
const fs = require('fs').promises;
const path = require('path');

const WEBFLOW_API_TOKEN = process.env.WEBFLOW_API_TOKEN;
const WEBFLOW_COLLECTION_ID = process.env.WEBFLOW_COLLECTION_ID;
const GOOGLE_SERVICE_ACCOUNT = process.env.GOOGLE_SERVICE_ACCOUNT;
const GOOGLE_SHEET_ID = process.env.GOOGLE_SHEET_ID;
const COINGECKO_API_URL = 'https://api.coingecko.com/api/v3/coins/markets';

// Main function
async function syncCoinsToWebflow() {
  try {
    console.log('Starting sync...');
    
    // Debug: Fetch collection schema to see field definitions
    console.log('Fetching collection schema...');
    const schemaResponse = await fetch(
      `https://api.webflow.com/v2/collections/${WEBFLOW_COLLECTION_ID}`,
      {
        headers: {
          'Authorization': `Bearer ${WEBFLOW_API_TOKEN}`,
          'accept': 'application/json'
        }
      }
    );
    
    if (schemaResponse.ok) {
      const schema = await schemaResponse.json();
      console.log('\n📋 Collection Fields:');
      schema.fields.forEach(field => {
        console.log(`  - ${field.displayName}: "${field.slug}" (${field.type})`);
      });
      console.log('');
    }
    
    // Step 1: Fetch top 50 coins from CoinGecko
    const coins = await fetchTopCoins();
    console.log(`Fetched ${coins.length} coins from CoinGecko`);
    
    // Step 2: Get existing items from Webflow
    const existingItems = await getAllWebflowItems();
    console.log(`Found ${existingItems.length} existing items in Webflow`);
    
    // Step 3: Sync data to Webflow
    const result = await syncItems(coins, existingItems);
    console.log('Webflow CMS updated successfully');
    
    // Step 4: Update Google Sheets
    console.log('\nUpdating Google Sheets...');
    await updateGoogleSheets(coins);
    console.log('Google Sheets updated successfully');
    
    // Step 5: Fetch chart data in rotating batches to avoid rate limits
    console.log('\nCleaning up old chart files...');
    await cleanupOldChartFiles(coins);
    
    // Determine which batch to fetch based on current UTC hour
    // Runs every 2 hours, 7 batches to cover all 50 coins
    const currentHour = new Date().getUTCHours(); // 0-23
    const batchNumber = Math.floor(currentHour / 2) % 7; // 0-6 (7 batches)
    const batchSize = 7;
    const startIdx = batchNumber * batchSize;
    const endIdx = Math.min(startIdx + batchSize, coins.length);
    const batchCoins = coins.slice(startIdx, endIdx);
    
    console.log(`Fetching chart data for batch ${batchNumber + 1}/7 (coins ${startIdx + 1}-${endIdx})...`);
    console.log(`Coins in this batch: ${batchCoins.map(c => c.symbol).join(', ')}`);
    await fetchAndSaveChartData(batchCoins);
    console.log(`Chart data saved for ${batchCoins.length} coins`);
    
    // Step 6: Log results
    logResults(result);
    
    return result;
  } catch (error) {
    console.error('Error during sync:', error.message);
    throw error;
  }
}

// Fetch top 50 coins from CoinGecko
async function fetchTopCoins() {
  const params = new URLSearchParams({
    vs_currency: 'usd',
    order: 'market_cap_desc',
    per_page: 50,
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
    
    // Debug: Show field names from first item
    if (offset === 0 && data.items.length > 0) {
      console.log('\n📋 Available fields in Webflow:');
      console.log(Object.keys(data.items[0].fieldData).join(', '));
      console.log('');
    }
    
    if (data.items.length < limit) {
      break;
    }
    
    offset += limit;
  }
  
  return items;
}

// Delete all items from Webflow
async function deleteAllItems(items) {
  for (let i = 0; i < items.length; i++) {
    const item = items[i];
    console.log(`  Deleting [${i + 1}/${items.length}]: ${item.fieldData?.name || item.id}`);
    
    try {
      const response = await fetch(
        `https://api.webflow.com/v2/collections/${WEBFLOW_COLLECTION_ID}/items/${item.id}`,
        {
          method: 'DELETE',
          headers: {
            'Authorization': `Bearer ${WEBFLOW_API_TOKEN}`,
            'accept': 'application/json'
          }
        }
      );
      
      if (!response.ok) {
        console.error(`    ❌ Failed to delete: ${response.status}`);
      }
      
      // Small delay to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 100));
    } catch (error) {
      console.error(`    ❌ Error: ${error.message}`);
    }
  }
}

// Sync items to Webflow - create all items (existing ones already deleted)
async function syncItems(coins, existingItems) {
  const results = { updated: 0, created: 0, failed: 0 };
  
  console.log(`\nCreating ${coins.length} coins in Webflow...\n`);
  
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
        'change-1y': coin.price_change_percentage_1y_in_currency?.toString() || '0',
        'market-cap': coin.market_cap?.toString() || '0',
        'volume-24h': coin.total_volume?.toString() || '0',
        'circulating-supply': coin.circulating_supply?.toString() || '0',
        'total-supply': coin.total_supply?.toString() || '0',
        'ath-usd-3': coin.ath?.toString() || '0',
        'atl-usd-3': coin.atl?.toString() || '0',
        'coingecko-id': coingeckoId,
        'last-updated': new Date().toISOString()
      };
      
      // Create new item (we already deleted all existing items)
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
        } else {
          const errorText = await response.text();
          console.error(`  ❌ Failed (${response.status}): ${errorText}`);
          results.failed++;
        }
      
      // Small delay to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 100));
      
    } catch (error) {
      console.error(`[${i + 1}/${coins.length}] Error: ${error.message}`);
      results.failed++;
    }
  }
  
  return results;
}

// Clean up chart files for coins no longer in top 50
async function cleanupOldChartFiles(currentCoins) {
  const dataDir = path.join(process.cwd(), 'data', 'charts');
  const currentIds = new Set(currentCoins.map(c => c.id));
  
  try {
    // Create directory if it doesn't exist
    await fs.mkdir(dataDir, { recursive: true });
    
    // Read existing files
    const files = await fs.readdir(dataDir);
    let deletedCount = 0;
    
    for (const file of files) {
      if (!file.endsWith('.json')) continue;
      
      const coinId = file.replace('.json', '');
      if (!currentIds.has(coinId)) {
        await fs.unlink(path.join(dataDir, file));
        console.log(`  🗑️  Deleted: ${file}`);
        deletedCount++;
      }
    }
    
    if (deletedCount === 0) {
      console.log('  ✅ No old files to clean up');
    } else {
      console.log(`  ✅ Cleaned up ${deletedCount} old file(s)`);
    }
    
  } catch (error) {
    console.error('  ⚠️  Cleanup error:', error.message);
  }
}

// Fetch and save chart data for coins
async function fetchAndSaveChartData(coins) {
  // Create data directory if it doesn't exist
  const dataDir = path.join(process.cwd(), 'data', 'charts');
  try {
    await fs.mkdir(dataDir, { recursive: true });
  } catch (error) {
    console.error('Error creating data directory:', error.message);
  }
  
  for (let i = 0; i < coins.length; i++) {
    const coin = coins[i];
    console.log(`[${i + 1}/${coins.length}] Fetching charts for ${coin.name}...`);
    
    try {
      // Fetch 365 days of data (includes price, market cap, volume)
      const response = await fetch(
        `https://api.coingecko.com/api/v3/coins/${coin.id}/market_chart?vs_currency=usd&days=365`
      );
      
      if (!response.ok) {
        console.error(`  ❌ Failed to fetch chart data: ${response.status}`);
        continue;
      }
      
      const chartData = await response.json();
      
      // Downsample the data
      const prices7d = downsampleData(chartData.prices.slice(-168), 28);
      const marketCaps7d = downsampleData(chartData.market_caps.slice(-168), 28);
      const volumes7d = downsampleData(chartData.total_volumes.slice(-168), 28);
      
      const prices30d = downsampleData(chartData.prices.slice(-720), 10);
      const marketCaps30d = downsampleData(chartData.market_caps.slice(-720), 10);
      const volumes30d = downsampleData(chartData.total_volumes.slice(-720), 10);
      
      const prices1y = downsampleData(chartData.prices, 12);
      const marketCaps1y = downsampleData(chartData.market_caps, 12);
      const volumes1y = downsampleData(chartData.total_volumes, 12);
      
      // Process and downsample the data
      const processed = {
        coingecko_id: coin.id,
        name: coin.name,
        symbol: coin.symbol.toUpperCase(),
        charts: {
          '7d': {
            prices: prices7d,
            prices_percent_change: calculatePercentageChange(prices7d),
            market_caps: marketCaps7d,
            market_caps_percent_change: calculatePercentageChange(marketCaps7d),
            volumes: volumes7d,
            volumes_percent_change: calculatePercentageChange(volumes7d),
            interval: '6_hours',
            points: 28
          },
          '30d': {
            prices: prices30d,
            prices_percent_change: calculatePercentageChange(prices30d),
            market_caps: marketCaps30d,
            market_caps_percent_change: calculatePercentageChange(marketCaps30d),
            volumes: volumes30d,
            volumes_percent_change: calculatePercentageChange(volumes30d),
            interval: '3_days',
            points: 10
          },
          '1y': {
            prices: prices1y,
            prices_percent_change: calculatePercentageChange(prices1y),
            market_caps: marketCaps1y,
            market_caps_percent_change: calculatePercentageChange(marketCaps1y),
            volumes: volumes1y,
            volumes_percent_change: calculatePercentageChange(volumes1y),
            interval: 'monthly',
            points: 12
          }
        },
        ath: coin.ath,
        atl: coin.atl,
        last_updated: new Date().toISOString()
      };
      
      // Save to file
      const filename = path.join(dataDir, `${coin.id}.json`);
      await fs.writeFile(filename, JSON.stringify(processed, null, 2));
      console.log(`  ✅ Saved chart data`);
      
      // Delay to respect rate limits
      // CoinGecko free tier is very strict - using 10 seconds to be safe
      await new Promise(resolve => setTimeout(resolve, 10000)); // 10 seconds
      
    } catch (error) {
      console.error(`  ❌ Error: ${error.message}`);
    }
  }
}

// Downsample data to target number of points
function downsampleData(data, targetPoints) {
  if (!data || data.length === 0) return [];
  if (data.length <= targetPoints) return data;
  
  const step = Math.floor(data.length / targetPoints);
  const result = [];
  
  for (let i = 0; i < data.length; i += step) {
    if (result.length < targetPoints) {
      result.push(data[i]);
    }
  }
  
  // Ensure we always include the last data point
  if (result.length < targetPoints && data.length > 0) {
    result.push(data[data.length - 1]);
  }
  
  return result;
}

// Calculate percentage change from baseline (first value)
function calculatePercentageChange(data) {
  if (!data || data.length === 0) return [];
  
  const baseline = data[0][1]; // First value
  if (!baseline || baseline === 0) return data.map(([ts]) => [ts, 0]);
  
  return data.map(([timestamp, value]) => {
    const percentChange = ((value - baseline) / baseline) * 100;
    return [timestamp, parseFloat(percentChange.toFixed(2))];
  });
}

// Update Google Sheets with coin data
async function updateGoogleSheets(coins) {
  if (!GOOGLE_SERVICE_ACCOUNT || !GOOGLE_SHEET_ID) {
    console.log('⚠️  Google Sheets not configured, skipping...');
    return;
  }
  
  try {
    const credentials = JSON.parse(GOOGLE_SERVICE_ACCOUNT);
    const auth = new google.auth.GoogleAuth({
      credentials,
      scopes: ['https://www.googleapis.com/auth/spreadsheets']
    });
    
    const sheets = google.sheets({ version: 'v4', auth });
    
    const headers = [
      'coingecko_id', 'name', 'symbol', 'logo_url', 'price', 
      'change_24h', 'change_7d', 'change_30d', 'market_cap', 
      'volume_24h', 'circulating_supply', 'total_supply',
      'ath_usd', 'atl_usd', 'last_updated'
    ];
    
    const rows = coins.map(coin => [
      coin.id,
      coin.name,
      coin.symbol?.toUpperCase(),
      coin.image,
      coin.current_price,
      coin.price_change_percentage_24h,
      coin.price_change_percentage_7d_in_currency,
      coin.price_change_percentage_30d_in_currency,
      coin.market_cap,
      coin.total_volume,
      coin.circulating_supply,
      coin.total_supply,
      coin.ath,
      coin.atl,
      new Date().toISOString()
    ]);
    
    await sheets.spreadsheets.values.clear({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: 'Sheet1!A:Z'
    });
    
    await sheets.spreadsheets.values.update({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: 'Sheet1!A1',
      valueInputOption: 'RAW',
      resource: {
        values: [headers, ...rows]
      }
    });
    
    console.log(`  ✅ Updated ${rows.length} rows`);
    
  } catch (error) {
    console.error('  ❌ Failed:', error.message);
  }
}

// Log results
function logResults(results) {
  console.log('\n' + '='.repeat(50));
  console.log('SYNC RESULTS');
  console.log('='.repeat(50));
  console.log(`✅ Webflow Updated: ${results.updated}`);
  console.log(`✨ Webflow Created: ${results.created}`);
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
