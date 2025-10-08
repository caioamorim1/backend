import { DataSource, Repository } from "typeorm";
import { UnidadeNaoInternacao } from "../entities/UnidadeNaoInternacao";
import { SitioFuncional } from "../entities/SitioFuncional";
import { CargoUnidade } from "../entities/CargoUnidade";
import {
  CriarUnidadeNaoInternacaoDTO,
  AtualizarUnidadeNaoInternacaoDTO,
} from "../dto/unidadeNaoInternacao.dto";

export class UnidadeNaoInternacaoRepository {
  private repo: Repository<UnidadeNaoInternacao>;
  private sitioRepo: Repository<SitioFuncional>;
  private cargoUnidadeRepo: Repository<CargoUnidade>;

  constructor(private ds: DataSource) {
    this.repo = ds.getRepository(UnidadeNaoInternacao);
    this.sitioRepo = ds.getRepository(SitioFuncional);
    this.cargoUnidadeRepo = ds.getRepository(CargoUnidade);
  }

  async criar(
    dados: CriarUnidadeNaoInternacaoDTO
  ): Promise<UnidadeNaoInternacao> {
    return await this.ds.transaction(async (manager) => {
      // 1. Criar a unidade
      const unidade = manager.create(UnidadeNaoInternacao, {
        hospital: { id: dados.hospitalId },
        nome: dados.nome,
        descricao: dados.descricao,
        horas_extra_reais: dados.horas_extra_reais,
        horas_extra_projetadas: dados.horas_extra_projetadas,
      });

      const unidadeSalva = await manager.save(UnidadeNaoInternacao, unidade);

      // 2. Criar sítios funcionais se fornecidos
      if (dados.sitios_funcionais?.length) {
        const sitios = dados.sitios_funcionais.map((sitio) =>
          manager.create(SitioFuncional, {
            unidade: unidadeSalva,
            nome: sitio.nome,
            descricao: sitio.descricao,
            especificacoes: sitio.especificacoes,
            tempo_padrao_procedimento: sitio.tempo_padrao_procedimento,
          })
        );
        await manager.save(SitioFuncional, sitios);
      }

      // 3. Criar relacionamentos cargo-unidade se fornecidos
      if (dados.cargos_unidade?.length) {
        const cargosUnidade = dados.cargos_unidade.map((cargo) =>
          manager.create(CargoUnidade, {
            cargoId: cargo.cargoId,
            unidadeNaoInternacaoId: unidadeSalva.id,
            quantidade_funcionarios: cargo.quantidade_funcionarios,
          })
        );
        await manager.save(CargoUnidade, cargosUnidade);
      }

      // 4. Retornar com relacionamentos
      return await manager.findOneOrFail(UnidadeNaoInternacao, {
        where: { id: unidadeSalva.id },
        relations: [
          "hospital",
          "sitiosFuncionais",

          "cargosUnidade",
          "cargosUnidade.cargo",
        ],
      });
    });
  }

  async listar(hospitalId?: string): Promise<UnidadeNaoInternacao[]> {
    const queryBuilder = this.repo
      .createQueryBuilder("unidade")
      .leftJoinAndSelect("unidade.hospital", "hospital")
      .leftJoinAndSelect("unidade.sitiosFuncionais", "sitios")
      .leftJoinAndSelect("sitios.posicoes", "posicoes")
      .leftJoinAndSelect("unidade.cargosUnidade", "cargosUnidade")
      .leftJoinAndSelect("cargosUnidade.cargo", "cargo");

    if (hospitalId) {
      queryBuilder.where("unidade.hospitalId = :hospitalId", { hospitalId });
    }

    return await queryBuilder.getMany();
  }

  async obter(id: string): Promise<UnidadeNaoInternacao | null> {
    return await this.repo.findOne({
      where: { id },
      relations: [
        "hospital",
        "sitiosFuncionais",
        "cargosUnidade",
        "cargosUnidade.cargo",
      ],
    });
  }

  async atualizar(
    id: string,
    dados: AtualizarUnidadeNaoInternacaoDTO
  ): Promise<UnidadeNaoInternacao | null> {
    return await this.ds.transaction(async (manager) => {
      const unidade = await manager.findOne(UnidadeNaoInternacao, {
        where: { id },
        relations: ["sitiosFuncionais", "cargosUnidade"],
      });

      if (!unidade) return null;

      // 1. Atualizar dados básicos da unidade
      await manager.update(UnidadeNaoInternacao, id, {
        nome: dados.nome,
        descricao: dados.descricao,
        horas_extra_reais: dados.horas_extra_reais,
        horas_extra_projetadas: dados.horas_extra_projetadas,
      });

      // 2. Atualizar sítios funcionais (merge/upsert)
      // - se item.id => update
      // - se sem id => create
      // - por padrão NÃO remove itens não mencionados; para forçar remoção enviar dados.replace_sitios = true
      if (dados.sitios_funcionais !== undefined) {
        if (!Array.isArray(dados.sitios_funcionais)) {
          throw new Error("sitios_funcionais deve ser um array");
        }

        const incomingSitios = dados.sitios_funcionais;

        // Upsert incoming
        for (const s of incomingSitios) {
          const sAny = s as any;
          if (sAny.id) {
            const existing = await manager.findOne(SitioFuncional, {
              where: { id: sAny.id, unidade: { id } },
            });
            if (existing) {
              // Update only known fields in SitioFuncional
              await manager.update(
                SitioFuncional,
                { id: sAny.id },
                {
                  nome: sAny.nome ?? existing.nome,
                  descricao: sAny.descricao ?? existing.descricao,
                  status: sAny.status ?? existing.status,
                }
              );
            } else {
              const novo = manager.create(SitioFuncional, {
                unidade: { id },
                nome: sAny.nome,
                descricao: sAny.descricao,
              });
              await manager.save(SitioFuncional, novo);
            }
          } else {
            const sAny2 = s as any;
            const novo = manager.create(SitioFuncional, {
              unidade: { id },
              nome: sAny2.nome,
              descricao: sAny2.descricao,
            });
            await manager.save(SitioFuncional, novo);
          }
        }

        // If explicit replace requested, delete not-mentioned ones
        if ((dados as any).replace_sitios === true) {
          const keepIds = incomingSitios
            .map((x: any) => (x as any).id)
            .filter((id) => !!id);
          const existingIds = unidade.sitiosFuncionais?.map((s) => s.id) ?? [];
          const idsToRemove = existingIds.filter(
            (eid) => !keepIds.includes(eid)
          );
          if (idsToRemove.length) {
            await manager.delete(SitioFuncional, idsToRemove);
          }
        }
      }

      // 3. Atualizar cargos (merge/upsert). Semelhante aos sítios.
      if (dados.cargos_unidade !== undefined) {
        if (!Array.isArray(dados.cargos_unidade)) {
          throw new Error("cargos_unidade deve ser um array");
        }

        const incomingCargos = dados.cargos_unidade;
        for (const c of incomingCargos) {
          const cAny = c as any;
          if (cAny.id) {
            const existingCargo = await manager.findOne(CargoUnidade, {
              where: { id: cAny.id, unidadeNaoInternacaoId: id },
            });
            if (existingCargo) {
              await manager.update(
                CargoUnidade,
                { id: cAny.id },
                {
                  cargoId: cAny.cargoId ?? existingCargo.cargoId,
                  quantidade_funcionarios:
                    cAny.quantidade_funcionarios ??
                    existingCargo.quantidade_funcionarios,
                }
              );
            } else {
              const novo = manager.create(CargoUnidade, {
                cargoId: cAny.cargoId,
                unidadeNaoInternacaoId: id,
                quantidade_funcionarios: cAny.quantidade_funcionarios,
              });
              await manager.save(CargoUnidade, novo);
            }
          } else {
            const novo = manager.create(CargoUnidade, {
              cargoId: cAny.cargoId,
              unidadeNaoInternacaoId: id,
              quantidade_funcionarios: cAny.quantidade_funcionarios,
            });
            await manager.save(CargoUnidade, novo);
          }
        }

        if ((dados as any).replace_cargos === true) {
          const keepIds = incomingCargos
            .map((x: any) => (x as any).id)
            .filter((id) => !!id);
          const existingIds = unidade.cargosUnidade?.map((s) => s.id) ?? [];
          const idsToRemove = existingIds.filter(
            (eid) => !keepIds.includes(eid)
          );
          if (idsToRemove.length) {
            await manager.delete(CargoUnidade, idsToRemove);
          }
        }
      }

      // 4. Retornar unidade atualizada
      return await manager.findOne(UnidadeNaoInternacao, {
        where: { id },
        relations: [
          "hospital",
          "sitiosFuncionais",

          "cargosUnidade",
          "cargosUnidade.cargo",
        ],
      });
    });
  }

  async deletar(id: string): Promise<boolean> {
    const result = await this.repo.delete(id);
    return result.affected !== 0;
  }

  async listarPorHospital(hospitalId: string): Promise<UnidadeNaoInternacao[]> {
    return await this.repo.find({
      where: { hospital: { id: hospitalId } },
      relations: ["sitiosFuncionais", "cargosUnidade", "cargosUnidade.cargo"],
    });
  }
}
