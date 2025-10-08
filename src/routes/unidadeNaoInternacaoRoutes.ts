import { Router } from "express";
import { DataSource } from "typeorm";
import { UnidadeNaoInternacaoRepository } from "../repositories/unidadeNaoInternacaoRepository";
import { UnidadeNaoInternacaoController } from "../controllers/unidadeNaoInternacaoController";
import { SitioFuncionalRepository } from "../repositories/sitioFuncionalRepository";
import { SitioFuncionalController } from "../controllers/sitioFuncionalController";

export const UnidadeNaoInternacaoRoutes = (ds: DataSource): Router => {
  const r = Router();
  const repo = new UnidadeNaoInternacaoRepository(ds);
  const ctrl = new UnidadeNaoInternacaoController(repo, ds);

  // Rotas principais
  // Route: POST /unidades-nao-internacao
  r.post("/", ctrl.criar);
  r.get("/", ctrl.listar);
  r.get("/hospital/:hospitalId", ctrl.listarPorHospital);
  r.get("/:id", ctrl.obter);
  r.put("/:id", ctrl.atualizar);
  r.delete("/:id", ctrl.deletar);

  // Rotas específicas
  r.get("/:id/estatisticas", ctrl.estatisticas);

  // Rotas alias para compatibilidade com frontend que espera criar/atualizar sítios
  const sitioRepo = new SitioFuncionalRepository(ds);
  const sitioCtrl = new SitioFuncionalController(sitioRepo);

  // Criar sítio via unidade path (compatibilidade)
  r.post("/:unidadeId/sitios", async (req, res) => {
    // injeta unidadeId no body para o controller de sitio (que espera unidadeId no body)
    req.body = { ...(req.body || {}), unidadeId: req.params.unidadeId };
    return sitioCtrl.criar(req as any, res as any);
  });

  // Atualizar sítio via unidade path (compatibilidade)
  r.put("/:unidadeId/sitios/:sitioId", async (req, res) => {
    // rota do frontend inclui unidadeId no path; o controller de sitio espera PUT /sitios/sitios-funcionais/:id
    // redirecionamos chamando o controller de sitio com params.id = sitioId
    req.params = { ...(req.params || {}), id: req.params.sitioId } as any;
    return sitioCtrl.atualizar(req as any, res as any);
  });

  return r;
};
