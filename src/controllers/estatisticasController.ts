import { Request, Response } from "express";
import { DataSource } from "typeorm";
import { UnidadeNaoInternacao } from "../entities/UnidadeNaoInternacao";
import { SitioFuncional } from "../entities/SitioFuncional";

export class EstatisticasController {
  constructor(private dataSource: DataSource) {}

  // Relatório mensal de unidade de não-internação
  async relatorioMensalUnidade(req: Request, res: Response) {
    try {
      const { id } = req.params;
      const {
        mes = new Date().getMonth() + 1,
        ano = new Date().getFullYear(),
      } = req.query;

      const dataInicio = new Date(Number(ano), Number(mes) - 1, 1);
      const dataFim = new Date(Number(ano), Number(mes), 0);

      const unidadeRepository =
        this.dataSource.getRepository(UnidadeNaoInternacao);

      // Buscar unidade com seus sítios e posições
      const unidade = await unidadeRepository.findOne({
        where: { id: id },
        relations: ["sitiosFuncionais", "sitiosFuncionais.posicoes"],
      });

      if (!unidade) {
        return res.status(404).json({ message: "Unidade não encontrada" });
      }

      // Calcular estatísticas gerais
      const totalSitios = unidade.sitiosFuncionais.length;

      const relatorio = {
        unidade: {
          id: unidade.id,
          nome: unidade.nome,
        },
        periodo: {
          mes: Number(mes),
          ano: Number(ano),
          dataInicio,
          dataFim,
        },
        resumo: {
          totalSitios,
        },
        sitios: unidade.sitiosFuncionais.map((sitio: SitioFuncional) => ({
          id: sitio.id,
          nome: sitio.nome,
        })),
      };

      res.json(relatorio);
    } catch (error) {
      console.error("Erro ao gerar relatório mensal:", error);
      res.status(500).json({ message: "Erro interno do servidor" });
    }
  }

  // Estatísticas de um sítio funcional
  async estatisticasSitio(req: Request, res: Response) {
    try {
      const { id } = req.params;

      const sitioRepository = this.dataSource.getRepository(SitioFuncional);

      const sitio = await sitioRepository.findOne({
        where: { id: id },
        relations: ["unidade"],
      });

      if (!sitio) {
        return res
          .status(404)
          .json({ message: "Sítio funcional não encontrado" });
      }

      const estatisticas = {
        sitio: {
          id: sitio.id,
          nome: sitio.nome,
          descricao: sitio.descricao,
        },
        unidade: {
          id: sitio.unidade.id,
          nome: sitio.unidade.nome,
        },
      };

      res.json(estatisticas);
    } catch (error) {
      console.error("Erro ao obter estatísticas do sítio:", error);
      res.status(500).json({ message: "Erro interno do servidor" });
    }
  }
}
