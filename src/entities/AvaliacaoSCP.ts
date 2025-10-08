import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from "typeorm";
import { UnidadeInternacao } from "./UnidadeInternacao";
import { Leito } from "./Leito";
import { Colaborador } from "./Colaborador";

export enum ClassificacaoCuidado {
  MINIMOS = "MINIMOS",
  INTERMEDIARIOS = "INTERMEDIARIOS",
  ALTA_DEPENDENCIA = "ALTA_DEPENDENCIA",
  SEMI_INTENSIVOS = "SEMI_INTENSIVOS",
  INTENSIVOS = "INTENSIVOS",
}

// Novo status para uso como sessão de ocupação por leito
export enum StatusSessaoAvaliacao {
  ATIVA = "ATIVA",
  EXPIRADA = "EXPIRADA",
  LIBERADA = "LIBERADA",
}

@Entity("avaliacoes_scp")
@Index(["dataAplicacao", "unidade"])
export class AvaliacaoSCP {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @Column({ type: "date" })
  dataAplicacao!: string; // yyyy-mm-dd

  @ManyToOne(() => UnidadeInternacao, { nullable: true, onDelete: "SET NULL" })
  unidade!: UnidadeInternacao | null;

  // Campo opcional: avaliar diretamente o Leito para usar como sessão de ocupação
  @ManyToOne(() => Leito, { nullable: true, onDelete: "SET NULL" })
  leito!: Leito | null;

  @ManyToOne(() => Colaborador, { nullable: true, onDelete: "SET NULL" })
  autor!: Colaborador | null; // colaborador que realizou a avaliação

  @Column({ type: "varchar", length: 60 })
  scp!: string; // aceita enum legado e métodos dinâmicos

  // Prontuário mínimo, armazenado na avaliação/sessão (não no leito)
  @Column({ type: "varchar", length: 350, nullable: true })
  prontuario!: string | null;

  // Pontuação por item (flexível para DINI/Perroca/Fugulin); ex: {"alimentacao":2,"eliminacoes":3,...}
  @Column({ type: "jsonb" })
  itens!: Record<string, number>;

  @Column({ type: "int" })
  totalPontos!: number;

  @Column({ type: "enum", enum: ClassificacaoCuidado })
  classificacao!: ClassificacaoCuidado;

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;

  // Expiração da sessão (ex.: 24h após criação)
  @Column({ type: "timestamptz", nullable: true })
  expiresAt!: Date | null;

  // Status da sessão quando usado como ocupação de leito
  @Column({
    type: "enum",
    enum: StatusSessaoAvaliacao,
    nullable: true,
  })
  statusSessao!: StatusSessaoAvaliacao | null;
}
