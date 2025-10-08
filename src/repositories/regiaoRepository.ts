import { DataSource, Repository } from "typeorm";
import { Regiao } from "../entities/Regiao";
import { Grupo } from "../entities/Grupo";
import { CreateRegiaoDTO, AtualizarRegiaoDTO } from "../dto/regiao.dto";

export class RegiaoRepository {
  private repo: Repository<Regiao>;
  constructor(private ds: DataSource) {
    this.repo = ds.getRepository(Regiao);
  }

  criar(data: CreateRegiaoDTO) {
    return this.ds.transaction(async (manager) => {
      const repo = manager.getRepository(Regiao);
      const grupoRepo = manager.getRepository(Grupo);

      const entidade: any = { nome: data.nome };
      if (data.grupoId) {
        const grupo = await grupoRepo.findOne({ where: { id: data.grupoId } });
        if (!grupo) throw new Error("Grupo não encontrado");
        entidade.grupo = grupo;
      }

      const r = repo.create(entidade);
      return repo.save(r);
    });
  }

  buscarTodos() {
    return this.repo.find({ relations: ["hospitais", "grupo"] });
  }

  buscarPorId(id: string) {
    return this.repo.findOne({
      where: { id },
      relations: ["hospitais", "grupo"],
    });
  }

  async deletar(id: string) {
    const r = await this.repo.delete(id);
    return (r.affected ?? 0) > 0;
  }

  async atualizar(id: string, data: AtualizarRegiaoDTO) {
    return this.ds.transaction(async (manager) => {
      const repo = manager.getRepository(Regiao);
      const grupoRepo = manager.getRepository(Grupo);

      const existente = await repo.findOne({ where: { id } });
      if (!existente) return false;

      if (data.nome !== undefined) existente.nome = data.nome;

      if (data.grupoId !== undefined) {
        if (data.grupoId === null) {
          existente.grupo = null as any;
        } else {
          const grupo = await grupoRepo.findOne({
            where: { id: data.grupoId },
          });
          if (!grupo) throw new Error("Grupo não encontrado");
          existente.grupo = grupo as any;
        }
      }

      await repo.save(existente);
      return true;
    });
  }
}
