import { Request, Response } from "express";
import { ColetaRepository } from "../repositories/coletaRepository";
import { DataSource } from "typeorm";

export class ColetaController {
  constructor(private repo: ColetaRepository) {}

  criar = async (req: Request, res: Response) => {
    try {
      const body = req.body;
      const files = req.files as Express.Multer.File[];

      // ✅ CORREÇÃO APLICADA AQUI
      // Garantimos que 'respostas' seja um array antes de processá-lo.
      let respostasArray = [];
      if (body.respostas && typeof body.respostas === 'string') {
        try {
          respostasArray = JSON.parse(body.respostas);
        } catch (e) {
          return res.status(400).json({ error: "O campo 'respostas' não é um JSON válido." });
        }
      } else if (Array.isArray(body.respostas)) {
        respostasArray = body.respostas;
      }
      
      // Agora, mapeamos o array de respostas para adicionar as URLs das fotos
      if (Array.isArray(respostasArray) && files && files.length > 0) {
        body.respostas = respostasArray.map((resp: any, idx: number) => {
          // O backend espera o nome do campo como 'foto_0', 'foto_1', etc.
          const file = files.find((f) => f.fieldname === `foto_${idx}`);
          if (file) {
            // Adiciona a propriedade fotoUrl ao objeto de resposta
            return { ...resp, fotoUrl: `/uploads/coleta/${file.filename}` };
          }
          return resp;
        });
      }

      const coleta = await this.repo.criar(body);
      res.status(201).json(coleta);
    } catch (error) {
      console.error("Erro detalhado ao criar coleta:", error);
      res.status(400).json({
        error: "Erro ao criar coleta",
        details: error instanceof Error ? error.message : String(error),
      });
    }
  };

  listar = async (_: Request, res: Response) => {
    try {
      const coletas = await this.repo.listarTodos();
      res.json(coletas);
    } catch (error) {
      res.status(500).json({ error: "Erro ao buscar coletas" });
    }
  };

  buscarPorId = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const coleta = await this.repo.buscarPorId(id);
      if (!coleta)
        return res.status(404).json({ error: "Coleta não encontrada" });
      res.json(coleta);
    } catch (error) {
      res.status(500).json({ error: "Erro ao buscar coleta" });
    }
  };

  deletar = async (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const ok = await this.repo.deletar(id);
      if (!ok) return res.status(404).json({ error: "Coleta não encontrada" });
      res.status(204).send();
    } catch (error) {
      res.status(500).json({ error: "Erro ao deletar coleta" });
    }
  };

  listarPorHospital = async (req: Request, res: Response) => {
    try {
      const { hospitalId } = req.params;
      const coletas = await this.repo.listarPorHospital(hospitalId);
      res.json(coletas);
    } catch (error) {
      res.status(500).json({ error: "Erro ao buscar coletas do hospital" });
    }
  };
}