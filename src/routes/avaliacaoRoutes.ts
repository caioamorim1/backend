import { Router } from "express";
import { DataSource } from "typeorm";
import { AvaliacaoRepository } from "../repositories/avaliacaoRepository";
import { AvaliacaoController } from "../controllers/avaliacaoController";
import { ScpMetodoRepository } from "../repositories/scpMetodoRepository";

export const AvaliacaoRoutes = (ds: DataSource): Router => {
  const r = Router();
  const ctrl = new AvaliacaoController(
    new AvaliacaoRepository(ds),
    new ScpMetodoRepository(ds)
  );

  r.post("/", ctrl.criar); // criar avaliação (folha de coleta)
  // NOVO FLUXO DE SESSÃO/OCUPAÇÃO POR LEITO
  r.post("/sessao", ctrl.criarSessao); // body: { leitoId, unidadeId, scp, itens, colaboradorId, prontuario? }
  r.post("/sessao/:id/liberar", ctrl.liberarSessao);
  r.put("/sessao/:id", ctrl.atualizarSessao);
  r.get("/sessoes-ativas", ctrl.listarSessoesAtivas); // ?unidadeId=...
  r.get("/leitos-disponiveis", ctrl.leitosDisponiveis); // ?unidadeId=...
  r.get("/", ctrl.listarPorDia); // ?data=YYYY-MM-DD&unidadeId=...
  r.get("/todas", ctrl.listarTodas); // lista todas avaliações
  r.get("/unidade/:unidadeId", ctrl.listarPorUnidade); // lista por unidade
  r.get("/resumo-diario", ctrl.resumoDiario); // ?data=YYYY-MM-DD&unidadeId=...
  r.get("/consolidado-mensal", ctrl.consolidadoMensal); // ?unidadeId=...&ano=2025&mes=5
  r.get("/schema", ctrl.schema); // ?scp=FUGULIN|PERROCA|DINI
  r.get("/autor/:autorId", ctrl.buscarPorAutor);

  return r;
};
