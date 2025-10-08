import { Request, Response } from "express";
import { LeitosStatusService } from "../services/leitosStatusService";

export class LeitosStatusController {
  constructor(private service: LeitosStatusService) {}

  /**
   * PUT /leitos-status/unidade/:unidadeId
   * Atualiza o status de uma unidade específica
   */
  atualizarUnidade = async (req: Request, res: Response) => {
    try {
      const { unidadeId } = req.params;

      if (!unidadeId) {
        return res.status(400).json({ error: "unidadeId é obrigatório" });
      }

      const status = await this.service.atualizarStatusUnidade(unidadeId);
      return res.json({
        message: "Status da unidade atualizado com sucesso",
        data: status,
      });
    } catch (err) {
      return res.status(500).json({
        error: err instanceof Error ? err.message : String(err),
      });
    }
  };

  /**
   * PUT /leitos-status/hospital/:hospitalId
   * Atualiza o status de todas as unidades de um hospital
   */
  atualizarHospital = async (req: Request, res: Response) => {
    try {
      const { hospitalId } = req.params;

      if (!hospitalId) {
        return res.status(400).json({ error: "hospitalId é obrigatório" });
      }

      await this.service.atualizarStatusHospital(hospitalId);
      return res.json({
        message: "Status de todas as unidades do hospital atualizado",
      });
    } catch (err) {
      return res.status(500).json({
        error: err instanceof Error ? err.message : String(err),
      });
    }
  };

  /**
   * PUT /leitos-status/sync
   * Atualiza o status de todas as unidades do sistema
   */
  atualizarTodas = async (req: Request, res: Response) => {
    try {
      await this.service.atualizarTodasUnidades();
      return res.json({
        message: "Status de todas as unidades atualizado com sucesso",
      });
    } catch (err) {
      return res.status(500).json({
        error: err instanceof Error ? err.message : String(err),
      });
    }
  };
}
