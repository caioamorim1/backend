import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
  Unique,
  OneToOne,
  JoinColumn,
  OneToMany,
} from "typeorm";
import { ClassificacaoCuidado } from "./AvaliacaoSCP";
import { UnidadeInternacao } from "./UnidadeInternacao";

export interface ScpOption {
  label: string;
  value: number;
}

export interface ScpQuestion {
  key: string; // usado para mapear em itens da avaliação
  text: string;
  options: ScpOption[];
}

export interface ScpFaixa {
  min: number;
  max: number;
  classe: ClassificacaoCuidado;
}

@Entity("scp_metodos")
@Unique(["key"]) // chave única (ex.: FUGULIN, PERROCA, DINI, ...)
export class ScpMetodo {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @Index()
  @Column({ type: "varchar", length: 60 })
  key!: string; // UPPERCASE, sem espaços

  @Column({ type: "varchar", length: 160 })
  title!: string;

  @Column({ type: "text", nullable: true })
  description?: string | null;

  @OneToMany(() => UnidadeInternacao, (unidade) => unidade.scpMetodo)
  unidades!: UnidadeInternacao[];

  // perguntas e opções de pontuação
  @Column({ type: "jsonb" })
  questions!: ScpQuestion[];

  // faixas para classificar o total
  @Column({ type: "jsonb" })
  faixas!: ScpFaixa[];

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
