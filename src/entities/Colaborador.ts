import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  CreateDateColumn,
  UpdateDateColumn,
} from "typeorm";
import { Hospital } from "./Hospital";

@Entity("colaboradores")
export class Colaborador {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @ManyToOne(() => Hospital, (hospital) => hospital.colaboradores, {
    nullable: true,
  })
  hospital?: Hospital;

  @Column({ type: "varchar", length: 160 })
  nome!: string;

  @Column({ type: "varchar", length: 255, unique: true, nullable: true })
  email!: string;

  @Column({
    type: "enum",
    enum: ["ADMIN", "GESTOR", "COMUM"],
    default: "COMUM",
  })
  permissao!: "ADMIN" | "GESTOR" | "COMUM";

  @Column({ type: "varchar", length: 18, unique: true, nullable: true })
  cpf?: string;

  // senha (hash)
  @Column({ type: "varchar", length: 255, nullable: true })
  senha!: string | null;

  // Se true, exige troca de senha no próximo login (primeiro acesso)
  @Column({ type: "boolean", default: true })
  mustChangePassword!: boolean;

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
