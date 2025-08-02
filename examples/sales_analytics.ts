import { CIPClient } from "sfcc-cip-analytics-client/cip-client";
import {
  querySalesAnalytics,
  querySalesSummary,
  SalesMetrics,
  SalesSummaryRecord,
} from "sfcc-cip-analytics-client/data/aggregate/sales_analytics";

async function main() {
  const clientId = process.env.SFCC_CLIENT_ID;
  const clientSecret = process.env.SFCC_CLIENT_SECRET;
  const instance = process.env.SFCC_CIP_INSTANCE;
  const siteId = process.env.SFCC_SITE_ID || "Sites-NTOSFRA-Site";

  if (!clientId || !clientSecret || !instance) {
    throw new Error(
      "Required environment variables: SFCC_CLIENT_ID, SFCC_CLIENT_SECRET, SFCC_CIP_INSTANCE",
    );
  }

  console.log("Creating CIP client...");
  const client = new CIPClient(clientId, clientSecret, instance);
  console.log("Client created.");

  try {
    // Open connection
    console.log("\nOpening connection...");
    await client.openConnection({});
    console.log("Connection opened");

    // Example 1: Sales analytics for last 7 days
    const endDate = new Date(Date.parse('2024-04-01'));
    const startDate = new Date(Date.parse('2024-01-01'));

    console.log(
      `\nExample 1: Sales Analytics for ${siteId} from ${startDate.toISOString().split("T")[0]} to ${endDate.toISOString().split("T")[0]}...`,
    );

    const salesMetrics: SalesMetrics[] = [];

    const salesQuery = querySalesAnalytics(
      client,
      siteId,
      { startDate, endDate },
      50,
    );

    for await (const batch of salesQuery) {
      salesMetrics.push(...batch);
      console.log(
        `  Collected ${batch.length} sales metrics (total: ${salesMetrics.length})`,
      );
    }

    if (salesMetrics.length > 0) {
      console.log(`\nFound ${salesMetrics.length} days of sales data:`);
      
      // Debug: let's look at the first few records to see the data structure
      console.log("\n🔍 Debug: First sales metric record:");
      console.log(JSON.stringify(salesMetrics[0], null, 2));
      
      console.table(salesMetrics.map(metric => ({
        Date: new Date(metric.date).toISOString().split("T")[0],
        Revenue: `$${(Number(metric.std_revenue) || 0).toLocaleString()}`,
        Orders: (Number(metric.orders) || 0).toLocaleString(),
        AOV: `$${(Number(metric.std_aov) || 0).toFixed(2)}`,
        Units: (Number(metric.units) || 0).toLocaleString(),
        AOS: (Number(metric.aos) || 0).toFixed(1),
        Tax: `$${(Number(metric.std_tax) || 0).toLocaleString()}`,
        Shipping: `$${(Number(metric.std_shipping) || 0).toLocaleString()}`,
      })));

      // Calculate totals
      const totals = salesMetrics.reduce(
        (acc, metric) => ({
          revenue: acc.revenue + (Number(metric.std_revenue) || 0),
          orders: acc.orders + (Number(metric.orders) || 0),
          units: acc.units + (Number(metric.units) || 0),
          tax: acc.tax + (Number(metric.std_tax) || 0),
          shipping: acc.shipping + (Number(metric.std_shipping) || 0),
        }),
        { revenue: 0, orders: 0, units: 0, tax: 0, shipping: 0 },
      );

      console.log("\n📊 Period Summary:");
      console.log(`Total Revenue: $${totals.revenue.toLocaleString()}`);
      console.log(`Total Orders: ${totals.orders.toLocaleString()}`);
      console.log(`Average AOV: $${(totals.revenue / totals.orders).toFixed(2)}`);
      console.log(`Total Units: ${totals.units.toLocaleString()}`);
      console.log(`Average AOS: ${(totals.units / totals.orders).toFixed(1)}`);
      console.log(`Total Tax: $${totals.tax.toLocaleString()}`);
      console.log(`Total Shipping: $${totals.shipping.toLocaleString()}`);
    }

    // Example 2: Raw sales summary with device breakdown
    console.log(
      `\nExample 2: Sales Summary by Device for ${siteId} (last 3 days)...`,
    );

    const recentEndDate = new Date();
    const recentStartDate = new Date();
    recentStartDate.setDate(recentEndDate.getDate() - 3);

    const rawSalesData: SalesSummaryRecord[] = [];

    const rawSalesQuery = querySalesSummary(
      client,
      { startDate: recentStartDate, endDate: recentEndDate },
      { siteId },
      100,
    );

    for await (const batch of rawSalesQuery) {
      rawSalesData.push(...batch);
      console.log(
        `  Collected ${batch.length} raw sales records (total: ${rawSalesData.length})`,
      );
    }

    if (rawSalesData.length > 0) {
      // Group by device class
      const deviceBreakdown = rawSalesData.reduce(
        (acc, record) => {
          const device = record.device_class_code || 'Unknown';
          if (!acc[device]) {
            acc[device] = {
              revenue: 0,
              orders: 0,
              units: 0,
              records: 0,
            };
          }
          acc[device].revenue += (Number(record.std_revenue) || 0);
          acc[device].orders += (Number(record.num_orders) || 0);
          acc[device].units += (Number(record.num_units) || 0);
          acc[device].records += 1;
          return acc;
        },
        {} as Record<string, { revenue: number; orders: number; units: number; records: number }>,
      );

      console.log("\n📱 Sales by Device Type:");
      console.table(
        Object.entries(deviceBreakdown).map(([device, stats]) => ({
          Device: device,
          Revenue: `$${stats.revenue.toLocaleString()}`,
          Orders: stats.orders.toLocaleString(),
          Units: stats.units.toLocaleString(),
          AOV: `$${(stats.revenue / stats.orders).toFixed(2)}`,
          Records: stats.records,
        })),
      );

      // Group by registration status
      const registrationBreakdown = rawSalesData.reduce(
        (acc, record) => {
          const type = record.registered ? 'Registered' : 'Guest';
          if (!acc[type]) {
            acc[type] = {
              revenue: 0,
              orders: 0,
              units: 0,
            };
          }
          acc[type].revenue += (Number(record.std_revenue) || 0);
          acc[type].orders += (Number(record.num_orders) || 0);
          acc[type].units += (Number(record.num_units) || 0);
          return acc;
        },
        {} as Record<string, { revenue: number; orders: number; units: number }>,
      );

      console.log("\n👥 Sales by Customer Type:");
      console.table(
        Object.entries(registrationBreakdown).map(([type, stats]) => ({
          CustomerType: type,
          Revenue: `$${stats.revenue.toLocaleString()}`,
          Orders: stats.orders.toLocaleString(),
          Units: stats.units.toLocaleString(),
          AOV: `$${(stats.revenue / stats.orders).toFixed(2)}`,
          RevenueShare: `${((stats.revenue / Object.values(registrationBreakdown).reduce((sum, s) => sum + s.revenue, 0)) * 100).toFixed(1)}%`,
        })),
      );
    }

  } catch (error) {
    console.error("Error querying sales data:", error);
  } finally {
    // Always close the connection when done
    console.log("\nClosing connection...");
    await client.closeConnection();
    console.log("Connection closed.");
  }
}

main().catch((err) => {
  console.error("Failed to run sales analytics example:", err);
});
