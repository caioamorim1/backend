import { Request, Response } from "express";
import { DimensionamentoService } from "../services/dimensionamentoService";

export class DimensionamentoController {
  constructor(private service: DimensionamentoService) {}

  // Controller para rota de internação
  analiseInternacao = async (req: Request, res: Response) => {
    try {
      const { unidadeId } = req.params;
      const resultado = await this.service.calcularParaInternacao(unidadeId);
      res.json(resultado);
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Erro desconhecido";
      res.status(404).json({
        error: "Erro ao calcular análise para unidade de internação",
        details: message,
      });
    }
  };

  // Controller para rota de não internação
  analiseNaoInternacao = async (req: Request, res: Response) => {
    try {
      const { unidadeId } = req.params;
      console.log("Unidade ID after destructuring:", unidadeId);
      const resultado = await this.service.calcularParaNaoInternacao(unidadeId);
      res.json(resultado);
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Erro desconhecido";
      res.status(404).json({
        error: "Erro ao calcular análise para unidade de não internação",
        details: message,
      });
    }
  };
}
