import { connectToDatabase } from "../database/connection";
import { runSessionExpiryForDate } from "../jobs/sessionExpiry";
import { AppDataSource } from "../ormconfig";

async function main() {
  const dateArg = process.argv[2];
  if (!dateArg) {
    console.error("Uso: ts-node run-session-expiry.ts YYYY-MM-DD");
    process.exit(1);
  }

  await connectToDatabase();

  try {
    await runSessionExpiryForDate(AppDataSource as any, dateArg);
    console.log("Job executado para:", dateArg);
  } catch (e) {
    console.error("Erro ao executar job:", e);
    process.exit(2);
  } finally {
    // tenta desligar conexões
    try {
      await AppDataSource.destroy();
    } catch (e) {
      // ignore
    }
    process.exit(0);
  }
}

main();
