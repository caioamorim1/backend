import { DataSource, Repository } from "typeorm";
import { UnidadeInternacao } from "../entities/UnidadeInternacao";
import { CreateUnidadeDTO, UpdateUnidadeDTO } from "../dto/unidade.dto";
import { Hospital } from "../entities/Hospital";
import { ScpMetodo } from "../entities/ScpMetodo";
import { CargoUnidadeRepository } from "./cargoUnidadeRepository";

export class UnidadeRepository {
  private repo: Repository<UnidadeInternacao>;
  private hospitalRepo: Repository<Hospital>;
  private cargoUnidadeRepo: CargoUnidadeRepository;

  constructor(ds: DataSource) {
    this.repo = ds.getRepository(UnidadeInternacao);
    this.hospitalRepo = ds.getRepository(Hospital);
    this.cargoUnidadeRepo = new CargoUnidadeRepository(ds);
  }

  async criar(data: CreateUnidadeDTO) {
    try {
      const hospital = await this.hospitalRepo.findOneByOrFail({
        id: data.hospitalId,
      });

      // Validar cargos se fornecidos
      if (data.cargos_unidade && data.cargos_unidade.length > 0) {
        console.log("Validando cargos:", data.cargos_unidade);
        await this.cargoUnidadeRepo.validarCargosPertencemHospital(
          data.cargos_unidade,
          data.hospitalId
        );
      }

      return await this.repo.manager.transaction(async (trx) => {
        const uRepo = trx.getRepository(UnidadeInternacao);
        const lRepo = trx.getRepository(require("../entities/Leito").Leito);
        const mRepo = trx.getRepository(ScpMetodo);

        // Validate numeroLeitos is provided and is a non-negative integer
        const qtd = Number(data.numeroLeitos);
        if (Number.isNaN(qtd) || !Number.isInteger(qtd) || qtd < 0) {
          throw new Error(
            "numeroLeitos é obrigatório e deve ser inteiro não-negativo"
          );
        }

        // Optional: attach SCP method if provided
        let metodo: ScpMetodo | null = null;
        if (data.scpMetodoId) {
          metodo = await mRepo.findOne({ where: { id: data.scpMetodoId } });
          if (!metodo) throw new Error("scpMetodoId inválido");
        }

        const ent = uRepo.create({
          hospital,
          nome: data.nome,
          horas_extra_reais: data.horas_extra_reais,
          horas_extra_projetadas: data.horas_extra_projetadas,
          scpMetodo: metodo ?? null,
        });
        const unidade = await uRepo.save(ent);

        if (qtd > 0) {
          const novos = Array.from({ length: qtd }, (_, i) => {
            const numero = String(i + 1).padStart(3, "0");
            return lRepo.create({ numero, unidade });
          });
          await lRepo.save(novos);
        }

        // Criar cargos da unidade se fornecidos
        if (data.cargos_unidade && data.cargos_unidade.length > 0) {
          const cargoUnidadeRepo = trx.getRepository(
            require("../entities/CargoUnidade").CargoUnidade
          );
          const cargoRepo = trx.getRepository(
            require("../entities/Cargo").Cargo
          );

          for (const cargoData of data.cargos_unidade) {
            // Verificar se o cargo existe
            const cargo = await cargoRepo.findOneBy({ id: cargoData.cargoId });
            if (!cargo) {
              throw new Error(`Cargo ${cargoData.cargoId} não encontrado`);
            }

            const cargoUnidade = cargoUnidadeRepo.create({
              unidadeId: unidade.id,
              cargoId: cargoData.cargoId,
              quantidade_funcionarios: cargoData.quantidade_funcionarios,
            });

            await cargoUnidadeRepo.save(cargoUnidade);
          }
        }

        return await uRepo.findOneOrFail({
          where: { id: unidade.id },
          relations: [
            "leitos",
            "hospital",
            "scpMetodo",
            "cargosUnidade",
            "cargosUnidade.cargo",
          ],
        });
      });
    } catch (err: any) {
      console.error("Erro ao criar unidade:", err);
      throw err;
    }
  }

  async listar(hospitalId?: string) {
    const where: any = {};
    if (hospitalId) where.hospital = { id: hospitalId };

    const itens = await this.repo.find({
      where,
      relations: [
        "hospital",
        "leitos",
        "scpMetodo",
        "cargosUnidade",
        "cargosUnidade.cargo",
      ],
    });

    // Map items to include scpMetodoId and hospitalId explicitly
    return itens.map((u: any) => ({
      id: u.id,
      nome: u.nome,
      hospitalId: u.hospital?.id ?? null,
      horas_extra_reais: u.horas_extra_reais,
      horas_extra_projetadas: u.horas_extra_projetadas,
      leitos: u.leitos ?? [],
      cargos_unidade:
        u.cargosUnidade?.map((cu: any) => ({
          id: cu.id,
          cargoId: cu.cargoId,
          quantidade_funcionarios: cu.quantidade_funcionarios,
          cargo: {
            id: cu.cargo.id,
            nome: cu.cargo.nome,
            salario: cu.cargo.salario,
            carga_horaria: cu.cargo.carga_horaria,
            descricao: cu.cargo.descricao,
            adicionais_tributos: cu.cargo.adicionais_tributos,
          },
        })) ?? [],
      scpMetodoKey: u.scpMetodo ? u.scpMetodo.key : null,
      scpMetodoId: u.scpMetodo ? u.scpMetodo.id : null,
      created_at: u.created_at,
      updated_at: u.updated_at,
    }));
  }

  // retorna unidades (entidades completas) de um hospital — utilitário para estatísticas
  async listarPorHospital(hospitalId: string) {
    return this.repo.find({
      where: { hospital: { id: hospitalId } },
      relations: [
        "hospital",
        "leitos",
        "scpMetodo",
        "cargosUnidade",
        "cargosUnidade.cargo",
      ],
    });
  }

  async obter(id: string) {
    const u = await this.repo.findOne({
      where: { id },
      relations: [
        "hospital",
        "leitos",
        "scpMetodo",
        "cargosUnidade",
        "cargosUnidade.cargo",
      ],
    });
    if (!u) return null;
    return {
      id: u.id,
      nome: u.nome,
      hospitalId: u.hospital?.id ?? null,
      horas_extra_reais: u.horas_extra_reais,
      horas_extra_projetadas: u.horas_extra_projetadas,
      leitos: u.leitos ?? [],
      cargos_unidade:
        u.cargosUnidade?.map((cu: any) => ({
          id: cu.id,
          cargoId: cu.cargoId,
          quantidade_funcionarios: cu.quantidade_funcionarios,
          cargo: {
            id: cu.cargo.id,
            nome: cu.cargo.nome,
            salario: cu.cargo.salario,
            carga_horaria: cu.cargo.carga_horaria,
            descricao: cu.cargo.descricao,
            adicionais_tributos: cu.cargo.adicionais_tributos,
          },
        })) ?? [],
      scpMetodoKey: u.scpMetodo ? u.scpMetodo.key : null,
      scpMetodoId: u.scpMetodo ? u.scpMetodo.id : null,
      created_at: u.created_at,
      updated_at: u.updated_at,
    };
  }

  async atualizar(id: string, data: UpdateUnidadeDTO) {
    const unidade = await this.repo.findOne({
      where: { id },
      relations: ["hospital"],
    });
    if (!unidade) throw new Error("Unidade não encontrada");

    // Validar cargos se fornecidos
    if (data.cargos_unidade && data.cargos_unidade.length > 0) {
      await this.cargoUnidadeRepo.validarCargosPertencemHospital(
        data.cargos_unidade,
        unidade.hospital.id
      );
    }

    return await this.repo.manager.transaction(async (trx) => {
      const uRepo = trx.getRepository(UnidadeInternacao);

      // Atualizar campos básicos da unidade
      const updateData: any = {
        nome: data.nome,
        horas_extra_reais: data.horas_extra_reais,
        horas_extra_projetadas: data.horas_extra_projetadas,
      };

      // Atualizar SCP método se fornecido
      if (data.scpMetodoId !== undefined) {
        if (data.scpMetodoId) {
          const mRepo = trx.getRepository(ScpMetodo);
          const metodo = await mRepo.findOne({
            where: { id: data.scpMetodoId },
          });
          if (!metodo) throw new Error("scpMetodoId inválido");
          updateData.scpMetodo = metodo;
        } else {
          updateData.scpMetodo = null;
        }
      }

      await uRepo.update(id, updateData);

      // Atualizar cargos da unidade se fornecidos
      if (data.cargos_unidade !== undefined) {
        if (data.cargos_unidade.length > 0) {
          await this.cargoUnidadeRepo.substituirCargosPorUnidade(
            id,
            data.cargos_unidade
          );
        } else {
          // Se array vazio, remove todos os cargos
          await this.cargoUnidadeRepo.deletarPorUnidade(id);
        }
      }

      const novo = await this.obter(id);
      if (!novo) throw new Error("Unidade não encontrada após atualização");
      return novo;
    });
  }

  async deletar(id: string) {
    const r = await this.repo.delete(id);
    return (r.affected ?? 0) > 0;
  }
}
