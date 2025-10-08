import { DataSource, Repository } from "typeorm";
import { Rede } from "../entities/Rede";
import { CreateRedeDTO, AtualizarRedeDTO } from "../dto/rede.dto";

export class RedeRepository {
  private repo: Repository<Rede>;
  constructor(private ds: DataSource) {
    this.repo = ds.getRepository(Rede);
  }

  criar(data: CreateRedeDTO) {
    const r = this.repo.create(data as any);
    return this.repo.save(r);
  }

  buscarTodos() {
    return this.repo.find();
  }

  buscarPorId(id: string) {
    return this.repo.findOne({ where: { id } });
  }

  async deletar(id: string) {
    const r = await this.repo.delete(id);
    return (r.affected ?? 0) > 0;
  }

  async atualizar(id: string, data: AtualizarRedeDTO) {
    const result = await this.repo.update(id, data as any);
    return (result.affected ?? 0) > 0;
  }
}
