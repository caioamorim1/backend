import { DataSource, Repository } from "typeorm";
import { Grupo } from "../entities/Grupo";
import { Regiao } from "../entities/Regiao";
import { Rede } from "../entities/Rede";
import { CreateGrupoDTO, AtualizarGrupoDTO } from "../dto/grupo.dto";

export class GrupoRepository {
  private repo: Repository<Grupo>;
  constructor(private ds: DataSource) {
    this.repo = ds.getRepository(Grupo);
  }

  criar(data: CreateGrupoDTO) {
    return this.ds.transaction(async (manager) => {
      const repo = manager.getRepository(Grupo);
      const redeRepo = manager.getRepository(Rede);
      const regiaoRepo = manager.getRepository(Regiao);

      const grupo = repo.create({ nome: data.nome } as any);
      if (data.redeId) {
        const rede = await redeRepo.findOne({ where: { id: data.redeId } });
        if (!rede) throw new Error("Rede não encontrada");
        (grupo as any).rede = rede;
      }

      const savedEntity = (await repo.save(grupo)) as unknown as Grupo;

      return repo.findOne({
        where: { id: savedEntity.id },
        relations: ["rede", "regioes"],
      });
    });
  }

  buscarTodos() {
    return this.repo.find({ relations: ["rede", "regioes"] });
  }

  buscarPorId(id: string) {
    return this.repo.findOne({ where: { id }, relations: ["rede", "regioes"] });
  }

  async atualizar(id: string, data: AtualizarGrupoDTO) {
    return this.ds.transaction(async (manager) => {
      const repo = manager.getRepository(Grupo);
      const redeRepo = manager.getRepository(Rede);

      const existente = await repo.findOne({
        where: { id },
        relations: ["rede", "regioes"],
      });
      if (!existente) {
        throw new Error("Grupo não encontrado");
      }

      if (typeof data.nome === "string" && data.nome.trim().length > 0) {
        existente.nome = data.nome.trim();
      }

      if (data.redeId) {
        const novaRede = await redeRepo.findOne({ where: { id: data.redeId } });
        if (!novaRede) {
          throw new Error("Rede não encontrada");
        }
        existente.rede = novaRede;
      }

      await repo.save(existente);

      return repo.findOne({ where: { id }, relations: ["rede", "regioes"] });
    });
  }

  async deletar(id: string) {
    const r = await this.repo.delete(id);
    return (r.affected ?? 0) > 0;
  }
}
