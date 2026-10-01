// Example: using daxcache from an AWS Lambda function behind DAX.
//
// The Lambda must run inside the same VPC as the DAX cluster. The DAX endpoint
// comes from an environment variable, so the same code runs against direct
// DynamoDB locally (unset the variable) and DAX in production.
//
// See ../python/template.yaml for a SAM template with the required VPC config;
// point the Handler at this file and use the nodejs20.x runtime.

import { DynamoCache } from "daxcache";

const endpoints = process.env.DAX_ENDPOINT ? [process.env.DAX_ENDPOINT] : undefined;

const cache = new DynamoCache({
  tableName: process.env.CACHE_TABLE ?? "dynamocache-example",
  endpoints, // undefined => direct DynamoDB mode
});

const computeReport = cache.wrap(
  async (customerId) => {
    // Stand-in for an expensive query or downstream API call.
    return { customerId, score: customerId.length * 7 };
  },
  { ttlSeconds: 300, name: "computeReport" },
);

export async function handler(event) {
  const customerId = event?.customerId ?? "anonymous";
  const report = await computeReport(customerId);
  return { statusCode: 200, body: report };
}
