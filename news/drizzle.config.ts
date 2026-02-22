import { defineConfig } from "drizzle-kit";

export default defineConfig({
    schema: "./packages/db/schema.ts",
    out: "./drizzle",
    dialect: "postgresql",
    dbCredentials: {
        host: process.env.POSTGRES_HOST!,
        password: process.env.POSTGRES_PASSWORD!,
        user: process.env.POSTGRES_USER!,
        database: process.env.POSTGRES_DB!,
    },
});
