import { DataSource } from "typeorm";
import { QuestionarioRepository } from "../repositories/questionarioRepository";
import { coletaSchema } from "../utils/coletaSchema";
import crypto from "crypto";

export async function runColetaSeed(ds: DataSource) {
  try {
    const repo = new QuestionarioRepository(ds);

    for (const q of coletaSchema) {
      const exists = await repo.nomeJaExiste(q.nome);
      if (!exists) {
        // gera os ids das perguntas
        const perguntasComId = q.perguntas.map((p) => ({
          ...p,
          id: crypto.randomUUID(),
        }));

        await repo.criar({
          nome: q.nome,
          perguntas: perguntasComId,
        });

        console.log(`[SEED] Questionário criado: ${q.nome}`);
      } else {
        console.log(`[SEED] Questionário já existe: ${q.nome}`);
      }
    }
  } catch (err) {
    console.error("[SEED] Falha ao executar coletaSeed:", err);
  }
}
