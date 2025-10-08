import { DataSource, Repository } from "typeorm";
import { ParametrosNaoInternacao } from "../entities/ParametrosNaoInternacao";
import { UnidadeNaoInternacao } from "../entities/UnidadeNaoInternacao";
import {
  CreateParametrosNaoInternacaoDTO,
  UpdateParametrosNaoInternacaoDTO,
} from "../dto/parametrosNaoInternacao.dto";

export class ParametrosNaoInternacaoRepository {
  private repo: Repository<ParametrosNaoInternacao>;
  private unidadeRepo: Repository<UnidadeNaoInternacao>;

  constructor(ds: DataSource) {
    this.repo = ds.getRepository(ParametrosNaoInternacao);
    this.unidadeRepo = ds.getRepository(UnidadeNaoInternacao);
  }

  async obterPorUnidadeId(unidadeId: string) {
    return this.repo.findOne({
      where: { unidade: { id: unidadeId } },
      relations: ["unidade"],
    });
  }

  async upsert(
    unidadeId: string,
    data: CreateParametrosNaoInternacaoDTO | UpdateParametrosNaoInternacaoDTO
  ) {
    const unidade = await this.unidadeRepo.findOneByOrFail({ id: unidadeId });
    const existente = await this.obterPorUnidadeId(unidadeId);

    if (existente) {
      Object.assign(existente, data);
      return this.repo.save(existente);
    }

    const novo = this.repo.create({ unidade, ...data });
    return this.repo.save(novo);
  }

  async deletar(unidadeId: string) {
    const existente = await this.obterPorUnidadeId(unidadeId);
    if (!existente) return false;
    const result = await this.repo.delete(existente.id);
    return (result.affected ?? 0) > 0;
  }
}
