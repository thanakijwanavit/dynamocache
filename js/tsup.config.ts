import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm", "cjs"],
  dts: true,
  clean: true,
  sourcemap: true,
  // AWS SDK packages are peer-ish runtime deps and stay external.
  external: [
    "@amazon-dax-sdk/lib-dax",
    "@aws-sdk/client-dynamodb",
    "@aws-sdk/lib-dynamodb",
  ],
});
