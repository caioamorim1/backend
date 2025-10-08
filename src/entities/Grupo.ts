import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  ManyToOne,
  OneToMany,
} from "typeorm";
import { Rede } from "./Rede";
import { Regiao } from "./Regiao";

@Entity()
export class Grupo {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @Column({ type: "varchar", length: 255 })
  nome!: string;

  @CreateDateColumn({ type: "timestamp with time zone" })
  created_at!: Date;

  @UpdateDateColumn({ type: "timestamp with time zone" })
  updated_at!: Date;

  @ManyToOne(() => Rede, (rede) => rede.grupos, { nullable: false })
  rede!: Rede;

  @OneToMany(() => Regiao, (regiao: Regiao) => regiao.grupo)
  regioes!: Regiao[];
}
