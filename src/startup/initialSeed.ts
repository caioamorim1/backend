import { DataSource } from "typeorm";
import { ScpMetodoRepository } from "../repositories/scpMetodoRepository";
import { ColaboradorRepository } from "../repositories/colaboradorRepository";
import { runColetaSeed } from "./coletaSeed";
import bcrypt from "bcrypt";

// Executa seed automático apenas se ainda não existem os 3 métodos builtin
export async function runInitialScpMetodoSeed(ds: DataSource) {
  try {
    const repo = new ScpMetodoRepository(ds);
    const requiredKeys = ["FUGULIN", "PERROCA", "DINI"];
    let existing = 0;
    for (const k of requiredKeys) {
      const found = await repo.getByKey(k);
      if (found) existing++;
    }
    if (existing < requiredKeys.length) {
      console.log(
        `[SCP] Inicial: encontrados ${existing}/${requiredKeys.length}. Executando seedBuiltin()`
      );
      await repo.seedBuiltin();
      console.log("[SCP] Seed builtin concluído.");
    } else {
      console.log(
        "[SCP] Métodos builtin já presentes. Seed automático ignorado."
      );
    }

    // Criação do admin padrão se não existir
    const adminRepo = new ColaboradorRepository(ds);
    const admins = await adminRepo.listarAdmins();
    const exists = admins.some((a) => a.email === "admin@admin.com");
    if (!exists) {
      const pass = await bcrypt.hash("admin123", 10);
      await adminRepo.criarAdmin({
        email: "admin@admin.com",
        senha: pass,
        nome: "Administrador",
        cpf: "00000000000",
      });
      console.log("[SEED] Admin padrão criado: admin@admin.com / admin123");
    } else {
      console.log("[SEED] Admin padrão já existe, não será criado.");
    }

    await runColetaSeed(ds);
  } catch (e) {
    console.warn("[SCP] Falha ao executar seed inicial opcional:", e);
  }
}
