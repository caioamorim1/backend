import { DataSource, Repository } from "typeorm";
import { CargoUnidade } from "../entities/CargoUnidade";
import { Cargo } from "../entities/Cargo";
import {
  CreateCargoUnidadeDTO,
  UpdateCargoUnidadeDTO,
} from "../dto/cargoUnidade.dto";

export class CargoUnidadeRepository {
  private repo: Repository<CargoUnidade>;
  private cargoRepo: Repository<Cargo>;

  constructor(ds: DataSource) {
    this.repo = ds.getRepository(CargoUnidade);
    this.cargoRepo = ds.getRepository(Cargo);
  }

  async criar(unidadeId: string, data: CreateCargoUnidadeDTO) {
    // Verificar se o cargo existe
    const cargo = await this.cargoRepo.findOneBy({ id: data.cargoId });
    if (!cargo) {
      throw new Error("Cargo não encontrado");
    }

    // Verificar se já existe este cargo para esta unidade
    const existing = await this.repo.findOne({
      where: { unidadeId, cargoId: data.cargoId },
    });
    if (existing) {
      throw new Error("Este cargo já está associado a esta unidade");
    }

    const cargoUnidade = this.repo.create({
      unidadeId,
      cargoId: data.cargoId,
      quantidade_funcionarios: data.quantidade_funcionarios,
    });

    return await this.repo.save(cargoUnidade);
  }

  async listarPorUnidade(unidadeId: string) {
    return await this.repo.find({
      where: { unidadeId },
      relations: ["cargo"],
      order: { created_at: "ASC" },
    });
  }

  async obter(id: string) {
    const cargoUnidade = await this.repo.findOne({
      where: { id },
      relations: ["cargo", "unidade"],
    });
    if (!cargoUnidade) {
      throw new Error("CargoUnidade não encontrado");
    }
    return cargoUnidade;
  }

  async atualizar(id: string, data: UpdateCargoUnidadeDTO) {
    const cargoUnidade = await this.obter(id);

    if (data.cargoId && data.cargoId !== cargoUnidade.cargoId) {
      // Verificar se o novo cargo existe
      const cargo = await this.cargoRepo.findOneBy({ id: data.cargoId });
      if (!cargo) {
        throw new Error("Cargo não encontrado");
      }

      // Verificar se já existe este cargo para esta unidade
      const existing = await this.repo.findOne({
        where: {
          unidadeId: cargoUnidade.unidadeId,
          cargoId: data.cargoId,
        },
      });
      if (existing && existing.id !== id) {
        throw new Error("Este cargo já está associado a esta unidade");
      }
    }

    Object.assign(cargoUnidade, data);
    return await this.repo.save(cargoUnidade);
  }

  async deletar(id: string) {
    const result = await this.repo.delete(id);
    return result.affected === 1;
  }

  async deletarPorUnidade(unidadeId: string) {
    const result = await this.repo.delete({ unidadeId });
    return result.affected || 0;
  }

  async substituirCargosPorUnidade(
    unidadeId: string,
    cargos: CreateCargoUnidadeDTO[]
  ) {
    // Deletar todos os cargos existentes da unidade
    await this.deletarPorUnidade(unidadeId);

    // Criar os novos cargos
    const novosCargoUnidade = [];
    for (const cargoData of cargos) {
      const cargoUnidade = await this.criar(unidadeId, cargoData);
      novosCargoUnidade.push(cargoUnidade);
    }

    return novosCargoUnidade;
  }

  async validarCargosPertencemHospital(
    cargos: CreateCargoUnidadeDTO[],
    hospitalId: string
  ) {
    console.log(
      `Validando ${cargos.length} cargos para hospital ${hospitalId}`
    );

    for (const cargoData of cargos) {
      console.log(`Validando cargo ${cargoData.cargoId}`);

      const cargo = await this.cargoRepo.findOne({
        where: { id: cargoData.cargoId },
        relations: ["hospital"],
      });

      if (!cargo) {
        console.error(`Cargo ${cargoData.cargoId} não encontrado`);
        throw new Error(`Cargo ${cargoData.cargoId} não encontrado`);
      }

      console.log(
        `Cargo encontrado: ${cargo.nome}, hospitalId: ${cargo.hospitalId}, esperado: ${hospitalId}`
      );

      if (cargo.hospitalId !== hospitalId) {
        console.error(
          `Cargo ${cargo.nome} pertence ao hospital ${cargo.hospitalId}, não ao ${hospitalId}`
        );
        throw new Error(
          `O cargo ${cargo.nome} não pertence ao hospital da unidade`
        );
      }

      console.log(`Cargo ${cargo.nome} validado com sucesso`);
    }

    console.log("Todos os cargos validados com sucesso");
  }
}
