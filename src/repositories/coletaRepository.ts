import { DataSource, Repository } from "typeorm";
import { Coleta } from "../entities/Coleta";
import { CriarColetaDTO } from "../dto/coleta.dto";
import { Questionario } from "../entities/Questionario";
import { UnidadeInternacao } from "../entities/UnidadeInternacao";
import { SitioFuncional } from "../entities/SitioFuncional";
import { Hospital } from "../entities/Hospital";
import { Colaborador } from "../entities/Colaborador";

export class ColetaRepository {
  private repo: Repository<Coleta>;
  constructor(private ds: DataSource) {
    this.repo = ds.getRepository(Coleta);
  }

  async criar(dados: CriarColetaDTO): Promise<Coleta> {
    return await this.ds.transaction(async (manager) => {
      const questionario = await manager
        .getRepository(Questionario)
        .findOneOrFail({
          where: { id: dados.questionarioId },
        });
      let unidade: UnidadeInternacao | undefined;
      let sitio: SitioFuncional | undefined;
      let colaborador: Colaborador | undefined;
      if (dados.unidadeId) {
        unidade = await manager.getRepository(UnidadeInternacao).findOneOrFail({
          where: { id: dados.unidadeId },
        });
      }
      if (dados.sitioId) {
        sitio = await manager.getRepository(SitioFuncional).findOneOrFail({
          where: { id: dados.sitioId },
        });
      }
      if (dados.colaboradorId) {
        colaborador = await manager.getRepository(Colaborador).findOneOrFail({
          where: { id: dados.colaboradorId },
        });
      }
      const coleta = manager.create(Coleta, {
        questionario,
        unidade,
        sitio,
        localNome: dados.localNome,
        respostas: dados.respostas,
        colaborador,
      });
      return await manager.save(Coleta, coleta);
    });
  }

  async buscarPorId(id: string): Promise<Coleta | null> {
    return await this.repo.findOne({
      where: { id },
      relations: ["colaborador", "questionario"], // Adicionado questionario para detalhes
    });
  }

  async listarPorLocal(localId: string): Promise<Coleta[]> {
    return await this.repo.find({
      where: [{ unidade: { id: localId } }, { sitio: { id: localId } }],
      relations: ["colaborador", "questionario"],
    });
  }

  async listarPorHospital(hospitalId: string): Promise<Coleta[]> {
    // Busca todas as unidades de internação do hospital
    // ✅ CORREÇÃO: Removido `relations: ["colaborador"]` que causava o erro
    const unidades = await this.ds.getRepository(UnidadeInternacao).find({
      where: { hospital: { id: hospitalId } },
      select: ["id"],
    });
    const unidadeIds = unidades.map((u) => u.id);

    // Busca todas as unidades não internação do hospital
    // ✅ CORREÇÃO: Removido `relations: ["colaborador"]` que causava o erro
    const unidadesNaoInternacao = await this.ds
      .getRepository(
        require("../entities/UnidadeNaoInternacao").UnidadeNaoInternacao
      )
      .find({
        where: { hospital: { id: hospitalId } },
        select: ["id"],
      });
    const unidadeNaoInternacaoIds = unidadesNaoInternacao.map((u) => u.id);

    // Busca todos os sitios funcionais dessas unidades
    let sitioIds: string[] = [];
    if (unidadeNaoInternacaoIds.length > 0) {
      const sitios = await this.ds
        .getRepository(require("../entities/SitioFuncional").SitioFuncional)
        .find({
          where: unidadeNaoInternacaoIds.map((id) => ({ unidade: { id } })),
          select: ["id"],
        });
      sitioIds = sitios.map((s) => s.id);
    }

    // Busca todas as coletas dessas unidades e desses sitios
    const coletasUnidades =
      unidadeIds.length > 0
        ? await this.repo.find({
            where: unidadeIds.map((id) => ({ unidade: { id } })),
            relations: ["colaborador", "questionario"], // Carrega as relações aqui
            order: { created_at: "DESC" }
          })
        : [];
    const coletasSitios =
      sitioIds.length > 0
        ? await this.repo.find({
            where: sitioIds.map((id) => ({ sitio: { id } })),
            relations: ["colaborador", "questionario"], // Carrega as relações aqui
            order: { created_at: "DESC" }
          })
        : [];

    return [...coletasUnidades, ...coletasSitios].sort((a,b) => b.created_at.getTime() - a.created_at.getTime());
  }

  async listarTodos(): Promise<Coleta[]> {
    return await this.repo.find({ relations: ["colaborador", "questionario"] });
  }

  async deletar(id: string): Promise<boolean> {
    const result = await this.repo.delete(id);
    return result.affected !== 0;
  }
}