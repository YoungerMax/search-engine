import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import * as schema from "./schema";
import { env } from 'bun';

export * from "./schema";

export function createDb() {
  const connectionString = env.DATABASE_URL ?? 
    `postgres://${env.POSTGRES_USER}:${env.POSTGRES_PASSWORD}@${env.POSTGRES_HOST}:${env.POSTGRES_PORT ?? 5432}/${env.POSTGRES_DB}`;
  const client = postgres(connectionString);
  return drizzle(client, { schema });
}
