import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
} from "typeorm";

export type TipoPergunta =
  | "texto"
  | "numero"
  | "sim_nao"
  | "escala_1_5"
  | "escala_1_10";

export interface PerguntaFormulario {
  texto: string;
  tipo: TipoPergunta;
}

@Entity("formularios_coleta")
export class FormularioColeta {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @Column({ type: "varchar", length: 120 })
  nome!: string;

  @Column({ type: "text", nullable: true })
  descricao?: string;

  @Column({ type: "jsonb" })
  perguntas!: PerguntaFormulario[];

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
