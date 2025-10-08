import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  OneToMany,
} from "typeorm";
import { Coleta } from "./Coleta";

interface Pergunta {
  id: string;
  categoria: string;
  texto: string;
  tipoResposta: "sim_nao_na" | "texto" | "numero" | "data" | "multipla_escolha";
  opcoes?: string[]; // Para múltipla escolha
  obrigatoria: boolean;
}

@Entity("questionarios")
export class Questionario {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @OneToMany(() => Coleta, (coleta) => coleta.questionario)
  coletas?: Coleta[];

  @Column({ type: "varchar", length: 255 })
  nome!: string;

  @Column({ type: "json", nullable: false })
  perguntas!: Pergunta[];

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
