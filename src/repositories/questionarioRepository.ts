import { Repository, DataSource, FindManyOptions, ILike } from "typeorm";
import { Questionario } from "../entities/Questionario";
import {
  CreateQuestionarioDTO,
  UpdateQuestionarioDTO,
} from "../dto/questionario.dto";

export class QuestionarioRepository {
  private repository: Repository<Questionario>;

  constructor(dataSource: DataSource) {
    this.repository = dataSource.getRepository(Questionario);
  }

  async criar(dto: CreateQuestionarioDTO): Promise<Questionario> {
    // mapeia PerguntaDTO -> Pergunta (adicionando id)
    const perguntas = dto.perguntas.map((p) => ({
      id: crypto.randomUUID(),
      categoria: p.categoria,
      texto: p.texto,
      tipoResposta: p.tipoResposta,
      opcoes: p.opcoes,
      obrigatoria: p.obrigatoria,
    }));

    const novoQuestionario = this.repository.create({
      nome: dto.nome,
      perguntas,
    });

    return await this.repository.save(novoQuestionario);
  }

  async listarTodos(filtros?: {
    nome?: string;
    page?: number;
    limit?: number;
  }): Promise<{ questionarios: Questionario[]; total: number }> {
    const { nome, page = 1, limit = 10 } = filtros || {};

    const where: any = {};
    if (nome) {
      where.nome = ILike(`%${nome}%`);
    }

    const options: FindManyOptions<Questionario> = {
      where,
      order: { created_at: "DESC" },
      take: limit,
      skip: (page - 1) * limit,
    };

    const [questionarios, total] = await this.repository.findAndCount(options);
    return { questionarios, total };
  }

  async buscarPorId(id: string): Promise<Questionario | null> {
    return await this.repository.findOne({ where: { id } });
  }

  async buscarPorNome(nome: string): Promise<Questionario | null> {
    return await this.repository.findOne({ where: { nome: ILike(nome) } });
  }

  async atualizar(
    id: string,
    dto: UpdateQuestionarioDTO
  ): Promise<Questionario | null> {
    const questionario = await this.buscarPorId(id);
    if (!questionario) return null;

    if (dto.nome !== undefined) {
      questionario.nome = dto.nome;
    }

    if (dto.perguntas !== undefined) {
      questionario.perguntas = dto.perguntas.map((p) => ({
        id: crypto.randomUUID(),
        categoria: p.categoria,
        texto: p.texto,
        tipoResposta: p.tipoResposta,
        opcoes: p.opcoes,
        obrigatoria: p.obrigatoria,
      }));
    }

    return await this.repository.save(questionario);
  }

  async excluir(id: string): Promise<boolean> {
    const resultado = await this.repository.delete(id);
    return resultado.affected !== 0;
  }

  async contarTotal(): Promise<number> {
    return await this.repository.count();
  }

  async existe(id: string): Promise<boolean> {
    const count = await this.repository.count({ where: { id } });
    return count > 0;
  }

  async nomeJaExiste(nome: string, excluirId?: string): Promise<boolean> {
    const where: any = { nome: ILike(nome) };
    if (excluirId) {
      where.id = { $ne: excluirId };
    }
    const count = await this.repository.count({ where });
    return count > 0;
  }
}
