import { DataSource } from "typeorm";
import dotenv from "dotenv";
import path from "path";

dotenv.config({ path: ".env" });

const ext = process.env.NODE_ENV === "production" ? "js" : "ts";

export const AppDataSource = new DataSource({
  type: "postgres",
  host: process.env.DB_HOST,
  // Porta padrão do PostgreSQL é 5432; permite override via DB_PORT
  port: parseInt(process.env.DB_PORT || "5432"),
  username: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  entities: [
    path.join(__dirname, `entities/**/*.${ext}`),
    path.join(__dirname, `modules/qualitativo/entities/**/*.${ext}`),
  ],
  synchronize: true,
});
