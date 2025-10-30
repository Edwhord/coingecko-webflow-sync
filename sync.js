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
    
    // Step 3: Sync data to Webflow (update existing, create new)
    const result = await syncItems(coins, existingItems);
    console.log('Webflow CMS updated successfully');
    
    // Step 4: Archive coins that are no longer in top 50
    await archiveOldCoins(coins, existingItems);
    
    // Step 5: Update Google Sheets
    console.log('\nUpdating Google Sheets...');
    await updateGoogleSheets(coins);
    console.log('Google Sheets updated successfully');
    
    // Step 6: Fetch chart data in rotating batches to avoid rate limits
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
    
    // Step 7: Log results
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

// Archive coins that are no longer in top 50
async function archiveOldCoins(currentCoins, existingItems) {
  const currentCoinIds = new Set(currentCoins.map(c => c.id));
  const itemsToArchive = existingItems.filter(item => 
    !currentCoinIds.has(item.fieldData['coingecko-id'])
  );
  
  console.log(`\nArchiving ${itemsToArchive.length} coins no longer in top 50...`);
  
  for (let i = 0; i < itemsToArchive.length; i++) {
    const item = itemsToArchive[i];
    try {
      const response = await fetch(
        `https://api.webflow.com/v2/collections/${WEBFLOW_COLLECTION_ID}/items/${item.id}`,
        {
          method: 'PATCH',
          headers: {
            'Authorization': `Bearer ${WEBFLOW_API_TOKEN}`,
            'Content-Type': 'application/json',
            'accept': 'application/json'
          },
          body: JSON.stringify({ 
            isArchived: true 
          })
        }
      );
      
      if (response.ok) {
        console.log(`  ✅ Archived: ${item.fieldData.name}`);
      } else {
        const errorText = await response.text();
        console.error(`  ❌ Failed to archive ${item.fieldData.name}: ${errorText}`);
      }
      
      // Small delay to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 100));
    } catch (error) {
      console.error(`  ❌ Error archiving ${item.fieldData.name}: ${error.message}`);
    }
  }
  
  if (itemsToArchive.length === 0) {
    console.log('  ✅ No coins to archive');
  }
}

// Sync items to Webflow - update existing, create new ones
async function syncItems(coins, existingItems) {
  const results = { updated: 0, created: 0, failed: 0 };
  
  // Create a map of existing items by coingecko-id for easy lookup
  const existingItemsMap = new Map();
  existingItems.forEach(item => {
    const coingeckoId = item.fieldData['coingecko-id'];
    if (coingeckoId) {
      existingItemsMap.set(coingeckoId, item);
    }
  });
  
  console.log(`\nSyncing ${coins.length} coins to Webflow...\n`);
  
  for (let i = 0; i < coins.length; i++) {
    const coin = coins[i];
    const coingeckoId = coin.id;
    const existingItem = existingItemsMap.get(coingeckoId);
    
    try {
      const fieldData = {
        'name': coin.name || '',
        'symbol': coin.symbol?.toUpperCase() || '',
        'logo-url': coin.image || '',
        'price-2': coin.current_price?.toString() || '0',
        'change-24h-2': coin.price_change_percentage_24h?.toString() || '0',
        'change-7d-2': coin.price_change_percentage_7d_in_currency?.toString() || '0',
        'change-30d-2': coin.price_change_percentage_30d_in_currency?.toString() || '0',
        'change-1y-2': coin.price_change_percentage_1y_in_currency?.toString() || '0',
        'market-cap-2': coin.market_cap?.toString() || '0',
        'volume-24h-2': coin.total_volume?.toString() || '0',
        'circulating-supply-2': coin.circulating_supply?.toString() || '0',
        'total-supply-2': coin.total_supply?.toString() || '0',
        'ath-usd-4': coin.ath?.toString() || '0',
        'atl-usd-4': coin.atl?.toString() || '0',
        'coingecko-id': coingeckoId,
        'last-updated': new Date().toISOString()
      };
      
      if (existingItem) {
        // UPDATE existing item (preserves the item and its ID)
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
        } else {
          const errorText = await response.text();
          console.error(`  ❌ Failed to update (${response.status}): ${errorText}`);
          results.failed++;
        }
      } else {
        // CREATE new item (for coins that entered top 50)
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
          console.error(`  ❌ Failed to create (${response.status}): ${errorText}`);
          results.failed++;
        }
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
      // Fetch 90 days of data (better resolution for recent data)
      const response = await fetch(
        `https://api.coingecko.com/api/v3/coins/${coin.id}/market_chart?vs_currency=usd&days=90`
      );
      
      if (!response.ok) {
        console.error(`  ❌ Failed to fetch chart data: ${response.status}`);
        continue;
      }
      
      const chartData = await response.json();
      
      // Get current time for timestamp normalization
      const now = Date.now();
      
      // Process data with ALL points (no downsampling)
      const prices24h = getRecentData(chartData.prices, 24, now); // 24 points
      const prices7d = getRecentData(chartData.prices, 168, now); // 168 points
      const prices30d = getRecentData(chartData.prices, 720, now); // 720 points
      const prices1y = getRecentData(chartData.prices, 365, now, true); // 365 points (daily)

      const marketCaps24h = getRecentData(chartData.market_caps, 24, now);
      const marketCaps7d = getRecentData(chartData.market_caps, 168, now);
      const marketCaps30d = getRecentData(chartData.market_caps, 720, now);
      const marketCaps1y = getRecentData(chartData.market_caps, 365, now, true);
      
      const volumes24h = getRecentData(chartData.total_volumes, 24, now);
      const volumes7d = getRecentData(chartData.total_volumes, 168, now);
      const volumes30d = getRecentData(chartData.total_volumes, 720, now);
      const volumes1y = getRecentData(chartData.total_volumes, 365, now, true);

      // Process and save ALL data
      const processed = {
          coingecko_id: coin.id,
          name: coin.name,
          symbol: coin.symbol.toUpperCase(),
          charts: {
              '24h': {
                  prices: prices24h,
                  prices_percent_change: calculatePercentageChange(prices24h), // Accurate from full data
                  market_caps: marketCaps24h,
                  market_caps_percent_change: calculatePercentageChange(marketCaps24h),
                  volumes: volumes24h,
                  volumes_percent_change: calculatePercentageChange(volumes24h),
                  interval: 'hourly',
                  points: 24
              },
              '7d': {
                  prices: prices7d,
                  prices_percent_change: calculatePercentageChange(prices7d), // Accurate from full data
                  market_caps: marketCaps7d,
                  market_caps_percent_change: calculatePercentageChange(marketCaps7d),
                  volumes: volumes7d,
                  volumes_percent_change: calculatePercentageChange(volumes7d),
                  interval: 'hourly',
                  points: 168
              },
              '30d': {
                  prices: prices30d,
                  prices_percent_change: calculatePercentageChange(prices30d), // Accurate from full data
                  market_caps: marketCaps30d,
                  market_caps_percent_change: calculatePercentageChange(marketCaps30d),
                  volumes: volumes30d,
                  volumes_percent_change: calculatePercentageChange(volumes30d),
                  interval: 'hourly',
                  points: 720
              },
              '1y': {
                  prices: prices1y,
                  prices_percent_change: calculatePercentageChange(prices1y), // Accurate from full data
                  market_caps: marketCaps1y,
                  market_caps_percent_change: calculatePercentageChange(marketCaps1y),
                  volumes: volumes1y,
                  volumes_percent_change: calculatePercentageChange(volumes1y),
                  interval: 'daily',
                  points: 365
              }
          },
          ath: coin.ath,
          atl: coin.atl,
          last_updated: new Date().toISOString()
      };
      
      // Save to file
      const filename = path.join(dataDir, `${coin.id}.json`);
      await fs.writeFile(filename, JSON.stringify(processed, null, 2));
      console.log(`  ✅ Saved FULL chart data (${prices24h.length}/${prices7d.length}/${prices30d.length}/${prices1y.length} points)`);
      
      // Delay to respect rate limits
      // CoinGecko free tier is very strict - using 10 seconds to be safe
      await new Promise(resolve => setTimeout(resolve, 10000)); // 10 seconds
      
    } catch (error) {
      console.error(`  ❌ Error: ${error.message}`);
    }
  }
}

// Get recent data with normalized timestamps (no downsampling)
function getRecentData(data, pointsCount, baseTime, isOneYear = false) {
  if (!data || data.length === 0) return [];
  
  // Take the most recent data points
  const recentData = data.slice(-pointsCount);
  
  if (recentData.length === 0) return [];
  
  // Normalize timestamps to be relative to current time
  return normalizeTimestamps(recentData, pointsCount, baseTime, isOneYear);
}

// Process data for a specific time range with normalized timestamps
function processTimeRange(data, hoursBack, targetPoints, baseTime) {
  if (!data || data.length === 0) return { data: [], usedRecentData: false };
  
  // Take the most recent data points (assuming hourly data)
  const recentData = data.slice(-hoursBack);
  
  if (recentData.length === 0) return { data: [], usedRecentData: false };
  
  // Downsample to target points
  const downsampled = downsampleData(recentData, targetPoints);
  
  // Normalize timestamps to be relative to current time
  const normalizedData = normalizeTimestamps(downsampled, hoursBack, baseTime);
  
  return {
    data: normalizedData,
    usedRecentData: recentData.length >= hoursBack * 0.8 // At least 80% of expected data
  };
}

// Normalize timestamps to be consistent across all batches
function normalizeTimestamps(data, pointsCount, baseTime, isOneYear = false) {
  if (!data || data.length === 0) return [];
  
  if (isOneYear) {
    // For 1-year data: start from exactly 1 year ago, use daily intervals
    const oneYearAgo = baseTime - (365 * 24 * 60 * 60 * 1000); // Exactly 1 year ago
    const interval = (365 * 24 * 60 * 60 * 1000) / data.length; // Daily intervals
    
    return data.map(([_, value], index) => {
      const timestamp = oneYearAgo + (index * interval);
      return [timestamp, value];
    });
  } else {
    // For other ranges: calculate hoursBack based on pointsCount
    let hoursBack;
    if (pointsCount === 24) hoursBack = 24; // 24h range
    else if (pointsCount === 168) hoursBack = 168; // 7d range (168 hours)
    else if (pointsCount === 720) hoursBack = 720; // 30d range (720 hours)
    else hoursBack = pointsCount; // fallback
    
    const startTime = baseTime - (hoursBack * 60 * 60 * 1000);
    const interval = (hoursBack * 60 * 60 * 1000) / data.length;
    
    return data.map(([_, value], index) => {
      const timestamp = startTime + (index * interval);
      return [timestamp, value];
    });
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
  if (!baseline || baseline === 0) return data.map(([ts, _]) => [ts, 0]);
  
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
